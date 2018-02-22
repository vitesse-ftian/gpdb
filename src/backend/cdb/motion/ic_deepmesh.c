/*-------------------------------------------------------------------------
 * ic_deepmesh.c
 *	   Interconnect code specific to DeepMesh transport.
 *
 * IDENTIFICATION
 *	    src/backend/cdb/motion/ic_deepmesh.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/execnodes.h"	/* Slice, SliceTable */
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "libpq/ip.h"
#include "utils/builtins.h"

#include "cdb/cdbselect.h"
#include "cdb/tupchunklist.h"
#include "cdb/ml_ipc.h"
#include "cdb/cdbvars.h"
#include "cdb/deepmesh.h"

#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>

static inline MotionConn *
getMotionConn(ChunkTransportStateEntry *pEntry, int iConn)
{
    Assert(pEntry);
    Assert(pEntry->conns);
    Assert(iConn < pEntry->numConns + pEntry->numPrimaryConns);

    return pEntry->conns + iConn;
}

/* The DeepMesh session id is created from gp_session_id and
 * gp_interconnect_id. The session is created by QD before
 * any QE starts.
 */
static inline uint64 getDmSessId()
{
    uint64 v = gp_session_id;

    return (v << 32) + gp_interconnect_id;
}

/* Endpoint ID is: <segment id>.<send_slice id>.<recv_slice_id><sender|recver>*/
static inline void getDmEpId(dm_ep_t *epId, int segment, int sendSlideId, int recvSlideId, bool isSender)
{
    Assert(epId);

    memset(epId, 0, sizeof(dm_ep_t));
    sprintf(epId->id, "%d.%d.%d.%s", segment, sendSlideId, recvSlideId, isSender?"sender":"recver");
    return;
}

static ChunkTransportStateEntry *startOutgoingConnections(ChunkTransportState *transportStates,
                         Slice *sendSlice,
                         int *pOutgoingCount);
static void startIncomingConnections(ChunkTransportState *transportStates,
                         Slice *recvSlice,
                         int *pIncomingCount);
static void sendRegisterMessages(ChunkTransportState *transportStates, ChunkTransportStateEntry *pEntry);
static void readRegisterMessages(ChunkTransportState *transportStates, Slice *recvSlice);

static TupleChunkListItem RecvTupleChunkFromAnyDeepMesh(MotionLayerState *mlStates,
                         ChunkTransportState *transportStates,
                         int16 motNodeID,
                         int16 *srcRoute);

static TupleChunkListItem RecvTupleChunkFromDeepMesh(ChunkTransportState *transportStates,
                         int16 motNodeID,
                         int16 srcRoute);

static void SendEosDeepMesh(MotionLayerState *mlStates, ChunkTransportState *transportStates,
             int motNodeID, TupleChunkListItem tcItem);

static bool SendChunkDeepMesh(MotionLayerState *mlStates, ChunkTransportState *transportStates,
             ChunkTransportStateEntry *pEntry, MotionConn *conn, TupleChunkListItem tcItem, int16 motionId);

static bool flushBufferDeepMesh(MotionLayerState *mlStates, ChunkTransportState *transportStates,
             ChunkTransportStateEntry *pEntry, MotionConn *conn, int16 motionId);

static void doSendStopMessageDeepMesh(ChunkTransportState *transportStates, int16 motNodeID);

/*
 * Initialize DeepMesh specific communication.
 */
void
InitMotionDeepMesh()
{
    if( 0 != dm_connect(Gp_interconnect_deepmesh_path)) {
        /* report error */
        ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                errmsg("Interconnect error initalizing DeepMesh motion layer."),
                errdetail("connect to agent with path %s errno %d errmsg %s",
                          Gp_interconnect_deepmesh_path, dm_errno(), dm_errmsg())));
    } else if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        ereport(DEBUG1, (errmsg("Interconnect DeepMesh: initialization successfully")));
    }

    return;
}

/* cleanup any DeepMesh-specific comms info */
void
CleanupMotionDeepMesh(void)
{
    /* disconnect with DeepMesh agent*/
    dm_disconnect();
    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
        ereport(DEBUG1, (errmsg("Interconnect DeepMesh: disconnect successfully")));

    return;
}

/* Cancel Check function invoked by DeepMesh operations. This allow user cancels the blocked 
 * DeepMesh functions by interrupt or others.
 */
static void readMsgCancelCheck(void *param)
{
    ChunkTransportState *transportStates  = (ChunkTransportState *)param;

    /* Check Interrupt */
    ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
    
    /* check to see if the dispatcher should cancel */
    if (Gp_role == GP_ROLE_DISPATCH) {
        checkForCancelFromQD(transportStates);
    }
}

/* Default Cancel Check function invoked by DeepMesh operations. This allow user cancels the blocked 
 * DeepMesh functions by interrupt or others.
 */
static void defaultCancelCheck(void *param)
{
    ChunkTransportState *transportStates  = (ChunkTransportState *)param;

    /* Check Interrupt */
    ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
}

/* Function readMsgFromConn() is used to read in the next full msg from the given
 * MotionConn.
 *
 * This call blocks until the packet is read in, and is part of a
 * global scheme where senders block until the entire message is sent, and
 * receivers block until the entire message is read. We should also handle any PG interrupts.
 *
 * PARAMETERS
 *	 conn - MotionConn to read the packet from.
 *
 */
static void
readMsgFromConn(MotionConn *conn, ChunkTransportState *transportStates)
{
    const char *msg = NULL;
    int   msgSize = 0;

    /* Read full msg from connection. dm_recv() will be blocked until full msg is read
     * or is cancelled by interrupted/cancelFromQD, or connection error occurs, peer ep leaves.
     */
    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        ereport(DEBUG1, (errmsg("Interconnect DeepMesh start to receive a chunk from conn sid %ld sender ep %s receiver ep %s",
                      getDmSessId(),
                      (char*)conn->dmPeerEp.id,
                      (char*)conn->dmLocalEp.id)));
    }

    dm_set_cancel_checker(readMsgCancelCheck, transportStates);
    msg = dm_recv(conn->dmEpHdlr, &conn->dmPeerEp, &msgSize, 0);
    dm_set_cancel_checker(defaultCancelCheck, transportStates);

    if( NULL == msg) {
        ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
            errmsg("Interconnect error reading an incoming msg."),
                errdetail("%s for sid %ld sender ep %s receiver ep %s errno %d errmsg %s: %m",
                      "dm_recv",
                      getDmSessId(),
                      (char*)conn->dmPeerEp.id,
                      (char*)conn->dmLocalEp.id,
                      dm_errno(),
                      dm_errmsg())));
    }

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        elog(DEBUG1, "Interconnect DeepMesh readMsgFromConn read %d bytes from conn sid %ld local sender %s receiver ep %s",
                      msgSize,
                      getDmSessId(),
                      (char*)conn->dmPeerEp.id,
                      (char*)conn->dmLocalEp.id);
    }

    /* validate msg */
    if( msgSize < PACKET_HEADER_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
            errmsg("Interconnect error read msg size less than packet head size ."),
                errdetail("for conn sid %ld sender ep %s receiver ep %s msgsize %d: %m",
                      getDmSessId(),
                      (char*)conn->dmPeerEp.id,
                      (char*)conn->dmLocalEp.id,
                      msgSize)));
    }

    memcpy(&conn->msgSize, msg, sizeof(uint32));

    if (msgSize != conn->msgSize || msgSize > Gp_max_packet_size ) {
        ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
            errmsg("Interconnect error read msg size is invalid ."),
                errdetail("for conn sid %ld sender ep %s receiver ep %s \
                          msgSize %d packMsgSize %d max_packet_size %d: %m",
                      getDmSessId(),
                      (char*)conn->dmPeerEp.id,
                      (char*)conn->dmLocalEp.id,
                      msgSize, conn->msgSize, Gp_max_packet_size)));
    }

    /* copy the data to pbuf */
    conn->recvBytes = msgSize;
    memcpy(conn->pBuff, msg, msgSize);
    conn->msgPos = conn->pBuff;

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        elog(DEBUG1, "Interconnect DeepMesh end read chunks %d bytes from conn sid %ld local sender %s receiver ep %s",
                      conn->msgSize,
                      getDmSessId(),
                      (char*)conn->dmPeerEp.id,
                      (char*)conn->dmLocalEp.id);
    }

    return;
}


/* Function startOutgoingConnections() is used to initially kick-off any outgoing
 * connections for mySlice. It creates sender endpoint for this sendSlice->receiveSlice Pair.
 *
 * This should not be called for root slices (i.e. QD ones) since they don't
 * ever have outgoing connections.
 *
 * PARAMETERS
 *  sendSlice - Slice that this process is member of.
 *
 * RETURNS
 *	 Initialized ChunkTransportState for the Sending Motion Node Id.
 */
static ChunkTransportStateEntry *
startOutgoingConnections(ChunkTransportState *transportStates,
                        Slice *sendSlice,
                        int *pOutgoingCount)
{
    ChunkTransportStateEntry *pEntry;
    MotionConn *conn;
    ListCell   *cell;
    Slice	   *recvSlice;
    CdbProcess *cdbProc;
    uint64      sid;
    dm_ep_t     localEp;

    *pOutgoingCount = 0;
    recvSlice = (Slice *) list_nth(transportStates->sliceTable->slices, sendSlice->parentIndex);
    adjustMasterRouting(recvSlice);

    /* Get EP info */
    sid = getDmSessId();
    getDmEpId(&localEp, Gp_segment, sendSlice->sliceIndex, recvSlice->sliceIndex, true);

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        ereport(DEBUG1, (errmsg("Interconnect seg%d slice%d setting up sending motion node ",
                    Gp_segment, sendSlice->sliceIndex)));
    }

    pEntry = createChunkTransportState(transportStates,
                                       sendSlice,
                                       recvSlice,
                                       list_length(recvSlice->primaryProcesses));

    /*
     * Setup a MotionConn entry for each of our outbound connections. create a virtual
     * connection to each receiving endpoint by setting peer endpoint ID. NB: Some
     * mirrors could be down & have no CdbProcess entry.
     */
    conn = pEntry->conns;
    foreach(cell, recvSlice->primaryProcesses) {
        cdbProc = (CdbProcess *) lfirst(cell);
        if (cdbProc) {
            conn->cdbProc = cdbProc;
            conn->pBuff = palloc(Gp_max_packet_size);
            conn->state = mcsSetupOutgoingConnection;
            conn->wakeup_ms = 0;
            conn->remoteContentId = cdbProc->contentid;

            /* create sender endpoint if not created yet. 
             * Only create once for each sendSlice->recvSlice pair
             */
            if(-1 == pEntry->dmEpHdlr) {
                pEntry->dmEpHdlr = dm_ep_join(sid, &localEp);
                if( -1 == pEntry->dmEpHdlr) {
                    /* report error */
                    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                errmsg("Interconnect error create sender endpoint."),
                                    errdetail("sid %ld, ep %s errno %d errmsg %s",
                                    sid, (char*)localEp.id, dm_errno(), dm_errmsg())));
                } else if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
                    ereport(DEBUG1, (errmsg("Interconnect create a sender endpoint sid %ld ep %s successfully.",
                                    sid, (char*)localEp.id)));
                }
                pEntry->dmLocalEp = localEp;
            }

            conn->dmEpHdlr = pEntry->dmEpHdlr;
            conn->dmLocalEp = localEp;
            getDmEpId(&conn->dmPeerEp, cdbProc->contentid, sendSlice->sliceIndex, recvSlice->sliceIndex, false);

            (*pOutgoingCount)++;
        }
        conn++;
    }

    return pEntry;
}

/* Function startIncomingConnections() is used to initially kick-off any incoming
 * connections for mySlice. It creates receiver endpoint for each sendSlice->receiveSlice Pair.
 *
 * PARAMETERS
 *  recvSlice - Slice that this process is member of.
 */
static void
startIncomingConnections(ChunkTransportState *transportStates,
                        Slice *recvSlice,
                        int *pIncomingCount)
{
    ListCell   *cell;
    uint64      sid = getDmSessId();

	/* Now we'll do setup for each of our Receiving Motion Nodes. */
    foreach(cell, recvSlice->children) {
        int totalNumProcs, activeNumProcs;
        int	childId = lfirst_int(cell);
        ChunkTransportStateEntry *pEntry;
        Slice	   *sendSlice;
        dm_ep_t     localEp;
        CdbProcess *cdbProc;
        MotionConn *conn;

        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            ereport(DEBUG1, (errmsg("Setting up RECEIVING motion node for sender %d", childId)));
        }

        sendSlice = (Slice *) list_nth(transportStates->sliceTable->slices, childId);

        /* If we're using directed-dispatch we have dummy primary-process entries, so we count the entries.*/
        activeNumProcs = 0;
        totalNumProcs = list_length(sendSlice->primaryProcesses);

        getDmEpId(&localEp, Gp_segment, sendSlice->sliceIndex, recvSlice->sliceIndex, false);
        pEntry = createChunkTransportState(transportStates, sendSlice, recvSlice, totalNumProcs);

        for (int i = 0; i < totalNumProcs; i++) {
            conn = pEntry->conns + i;
            cdbProc = list_nth(sendSlice->primaryProcesses, i);
            if (cdbProc) {
                conn->cdbProc = cdbProc;
                conn->state = mcsSetupIncomingConnection;
                conn->remoteContentId = cdbProc->contentid;

                /* create receiver endpoint if not created yet.
                 * Only create once for each sendSlice->recvSlice pair
                 */
                if(-1 == pEntry->dmEpHdlr) {
                    pEntry->dmEpHdlr = dm_ep_join(sid, &localEp);
                    if( -1 == pEntry->dmEpHdlr) {
                        /* report error */
                        ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                errmsg("Interconnect error create receiver endpoint."),
                                    errdetail("sid %ld, ep %s errno %d errmsg %s",
                                    sid, (char*)localEp.id, dm_errno(), dm_errmsg())));
                    } else if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
                        ereport(DEBUG1, (errmsg("Interconnect create a receiver endpoint sid %ld ep %s successfully.",
                                    sid, (char*)localEp.id)));
                    }
                    pEntry->dmLocalEp = localEp;
                }

                conn->dmEpHdlr = pEntry->dmEpHdlr;
                conn->dmLocalEp = localEp;
                getDmEpId(&conn->dmPeerEp, cdbProc->contentid, sendSlice->sliceIndex, recvSlice->sliceIndex, true);
                conn->isReceiver = true;

                activeNumProcs++;
            }
        }

        /* let cdbmotion now how many receivers to expect. */
        setExpectedReceivers(transportStates->estate->motionlayer_context, childId, activeNumProcs);

        *pIncomingCount += activeNumProcs;

        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
	    dumpEntryConnections(DEBUG4, pEntry);
        }
    }
}

/* Function sendRegisterMessages() used to send register messages each conns
 * which this process is the sender. The function will be blocked until all
 * registration msgs are send (wait for remote EPs starts) unless interrupted
 * or error occurs
 *
 * PARAMETERS
 *	 pEntry - ChunkTransportState.
 *
 * Called by SetupInterconnect
 *
 * On return, state is:
 *      mcsStarted if registration message has been sent.  Caller can start
 *          sending data.
 */
static void
sendRegisterMessages(ChunkTransportState *transportStates, ChunkTransportStateEntry *pEntry)
{
    int connNum =  pEntry ? pEntry->numConns : 0;

    for(int i = 0; i < connNum; i++) {
        MotionConn *conn = &pEntry->conns[i];

        if (conn->state == mcsSetupOutgoingConnection) {
            conn->state = mcsSendRegMsg; 
            RegisterMessage *regMsg = (RegisterMessage *) conn->pBuff;

            Assert(conn->cdbProc &&
               conn->pBuff &&
               sizeof(*regMsg) <= Gp_max_packet_size);
            Assert(pEntry->recvSlice &&
               pEntry->sendSlice &&
               IsA(pEntry->recvSlice, Slice) &&
               IsA(pEntry->sendSlice, Slice));

            if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE) {
                ereport(DEBUG1, (errmsg("Interconnect sending registration message "
                                 "to seg%d slice%d ep %s pid=%d "
                                 "from seg%d slice%d ep %s epHdlr=%d",
                                 conn->remoteContentId,
                                 pEntry->recvSlice->sliceIndex,
                                 (char*)conn->dmPeerEp.id,
                                 conn->cdbProc->pid,
                                 Gp_segment,
                                 pEntry->sendSlice->sliceIndex,
                                 (char*)conn->dmLocalEp.id,
                                 conn->dmEpHdlr)));
            }

            regMsg->msgBytes = sizeof(*regMsg);
            regMsg->recvSliceIndex = pEntry->recvSlice->sliceIndex;
            regMsg->sendSliceIndex = pEntry->sendSlice->sliceIndex;

            regMsg->srcContentId = Gp_segment;
            regMsg->srcListenerPort = Gp_listener_port & 0x0ffff;
            regMsg->srcPid = MyProcPid;
            regMsg->srcSessionId = gp_session_id;
            regMsg->srcCommandCount = gp_interconnect_id;

            if(0 == dm_send(conn->dmEpHdlr, &conn->dmPeerEp, (char*)regMsg, sizeof(*regMsg), DM_FLAG_WAIT_EP_JOIN)) {
                /* registration msg is sent successfully */

                if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE) {
                    ereport(DEBUG1, (errmsg("Interconnect registration message is sent successfully "
                                    "to seg%d slice%d ep %s pid=%d "
                                    "from seg%d slice%d ep %s epHdlr=%d",
                                    conn->remoteContentId,
                                    pEntry->recvSlice->sliceIndex,
                                    (char*)conn->dmPeerEp.id,
                                    conn->cdbProc->pid,
                                    Gp_segment,
                                    pEntry->sendSlice->sliceIndex,
                                    (char*)conn->dmLocalEp.id,
                                    conn->dmEpHdlr)));
                }

                conn->state = mcsStarted;
                conn->msgPos = NULL;
                conn->msgSize = PACKET_HEADER_SIZE;
                conn->stillActive = true;
            } else {
                ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                        errmsg("Interconnect sending registration message "
                             "to seg%d slice%d ep %s pid=%d " 
                             "from seg%d slice%d ep %s epHdlr=%d",
                             conn->remoteContentId,
                             pEntry->recvSlice->sliceIndex,
                             (char*)conn->dmPeerEp.id,
                             conn->cdbProc->pid,
                             Gp_segment,
                             pEntry->sendSlice->sliceIndex,
                             (char*)conn->dmLocalEp.id,
                             conn->dmEpHdlr),
                         errdetail("errno %d errmsg %s",
                             dm_errno(), dm_errmsg())));
            }
        }
    }/* for */
}


/* Function readRegisterMessages() reads "Register" message ceiver endpoint
 *
 * PARAMETERS
 *	 recvSlice - slice this process is in
 */
static void 
readRegisterMessages(ChunkTransportState *transportStates,
                     Slice *recvSlice)
{
    ListCell   *cell;
    uint64      sid = getDmSessId();

    foreach(cell, recvSlice->children) {
        int	childId = lfirst_int(cell);
        ChunkTransportStateEntry *pEntry = NULL;
        Slice	   *sendSlice;
        CdbProcess *cdbProc;
        MotionConn *conn;
        RegisterMessage *regMsg;
        RegisterMessage msg;

        sendSlice = (Slice *) list_nth(transportStates->sliceTable->slices, childId);
        getChunkTransportState(transportStates, sendSlice->sliceIndex, &pEntry);
        Assert(pEntry);

        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            ereport(DEBUG1, (errmsg("Reading registration msgs for sender %d, conn # %d", childId, pEntry->numConns)));
        }

        for (int i = 0; i < pEntry->numConns; i++) {
            conn = &pEntry->conns[i];

            if (mcsSetupIncomingConnection == conn->state) {
                /* connection has been initialized. now read registration msg from the conn
                 * dm_recv will be blocked until registration msg is read or interrupted.
                 */
                const char  *msgRead;
                int    msgSize;

                conn->state = mcsRecvRegMsg;
                msgRead = dm_recv(conn->dmEpHdlr, &conn->dmPeerEp, &msgSize, DM_FLAG_WAIT_EP_JOIN);
                if(NULL == msgRead) {
                    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                            errmsg("Interconnect error reading register message "
                                    "for conn sid %ld sender ep %s receiver ep %s ", 
                                    sid, (char*)conn->dmPeerEp.id, (char*)conn->dmLocalEp.id),
                            errdetail("dm_read errno %d errmsg %s ",
                                      dm_errno(),
                                      dm_errmsg())));
                }
        
                /* validate registration message first */
                regMsg = (RegisterMessage *) msgRead;
                msg.msgBytes = regMsg->msgBytes;

                if(msgSize != sizeof(*regMsg)|| msg.msgBytes != sizeof(*regMsg)) {
                    /* Report error */
                    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                        errmsg("Interconnect error reading register message "
                               "for conn sess_id %ld sender ep %s receiver ep %s format not recognized", 
                               sid, (char*)conn->dmPeerEp.id, (char*)conn->dmLocalEp.id),
                        errdetail("msgBytes=%d expected=%d readMsgSize=%d",
                                msg.msgBytes, (int) sizeof(*regMsg), msgSize)));
                }

                msg.recvSliceIndex = regMsg->recvSliceIndex;
                msg.sendSliceIndex = regMsg->sendSliceIndex;
                msg.srcContentId = regMsg->srcContentId;
                msg.srcListenerPort = regMsg->srcListenerPort;
                msg.srcPid = regMsg->srcPid;
                msg.srcSessionId = regMsg->srcSessionId;
                msg.srcCommandCount = regMsg->srcCommandCount;

	            /* Verify that the message pertains to one of our receiving Motion nodes. */
                if (msg.srcSessionId != gp_session_id ||
                    msg.srcCommandCount != gp_interconnect_id ||
                    !( msg.sendSliceIndex > 0 &&
                    msg.sendSliceIndex <= transportStates->size &&
                    msg.sendSliceIndex == sendSlice->sliceIndex &&
                    msg.recvSliceIndex == transportStates->sliceId &&
                    msg.srcContentId >= -1)) {
                    /* something is wrong */
                    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                        errmsg("Interconnect error: Invalid registration "
                               "message received from sid %ld receiver ep %s sender ep %s.",
                               getDmSessId(),
                               (char*)conn->dmLocalEp.id,
                               (char*)conn->dmPeerEp.id),
                        errdetail("sendSlice=%d recvSlice=%d srcContentId=%d "
                                  "srcPid=%d srcSessionId=%d srcCommandCount=%d motnode=%d",
                                  msg.sendSliceIndex, msg.recvSliceIndex,
                                  msg.srcContentId, msg.srcPid, msg.srcSessionId,
                                  msg.srcCommandCount, msg.sendSliceIndex)));
                    }


                /* verify the CdbProcess node for the sending process */
                cdbProc =  conn->cdbProc;
                if (msg.srcContentId != cdbProc->contentid || msg.srcPid != cdbProc->pid) {
                    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                        errmsg("Interconnect error: Invalid registration "
                               "for conn sess_id %ld sender ep %s receiver ep %s format not recognized", 
                               sid, (char*)conn->dmPeerEp.id, (char*)conn->dmLocalEp.id),
                        errdetail("sendSlice=%d srcContentId=%d srcPid=%d",
                               msg.sendSliceIndex, msg.srcContentId, msg.srcPid)));
                }

                /* message looks good */
                if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE) {
                    ereport(DEBUG1, (errmsg("Interconnect seg%d slice%d receiver ep %s receives "
                             "a valid registration message from seg%d slice%d sid %ld sender ep %s pid=%d",
                             Gp_segment,
                             msg.recvSliceIndex,
                             (char*)conn->dmLocalEp.id,
                             msg.srcContentId,
                             msg.sendSliceIndex,
                             sid,
                             (char*)conn->dmPeerEp.id,
                             msg.srcPid)));
                }

                /* set values of the connection */
                conn->pBuff = palloc(Gp_max_packet_size);
                conn->remapper = CreateTupleRemapper();
                conn->recvBytes = 0;
                conn->tupleCount = 0;
                conn->msgPos = NULL;
                conn->msgSize = 0;

                conn->state = mcsStarted;
                conn->stillActive = true;
                conn->isReceiver = true;
            } /*if*/

        } /* for pEntry */

        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
	    dumpEntryConnections(DEBUG4, pEntry);
        }
    }

} /* readRegisterMessages */

/* 
 * This function is to create virtual interconnections among QEs based on DM endpoints.
 * Each QE creates mutiple DeepMesh endpoints in same session( A session is created 
 * before SetupInterconnect and destoryed after it). Each ChunkTransportStateEntry maps
 * to one DM endpoint: either sender endpoint or receiver endpoint.
 */
void
SetupDeepMeshInterconnect(EState *estate)
{
    Slice      *mySlice;
    int         expectedTotalIncoming = 0;
    int         expectedTotalOutgoing = 0;
    GpMonotonicTime startTime;
    uint64		elapsed_ms = 0;

    /* we can have at most one of these. */
    ChunkTransportStateEntry *sendingChunkTransportState = NULL;

    if (estate->interconnect_context) {
        ereport(FATAL, (errmsg("SetupDeepMeshInterconnect: already initialized.")));
    } else if (!estate->es_sliceTable) {
        ereport(FATAL, (errmsg("SetupTCPInterconnect: no slice table ?")));
    }

    estate->interconnect_context = palloc0(sizeof(ChunkTransportState));

    estate->interconnect_context->estate = estate;

    /* initialize state variables */
    Assert(estate->interconnect_context->size == 0);
    estate->interconnect_context->size = CTS_INITIAL_SIZE;
    estate->interconnect_context->states = palloc0(CTS_INITIAL_SIZE * sizeof(ChunkTransportStateEntry));

    estate->interconnect_context->teardownActive = false;
    estate->interconnect_context->activated = false;
    estate->interconnect_context->incompleteConns = NIL;
    estate->interconnect_context->sliceTable = NULL;
    estate->interconnect_context->sliceId = -1;

    estate->interconnect_context->sliceTable = estate->es_sliceTable;
    estate->interconnect_context->sliceId = LocallyExecutingSliceIndex(estate);

    estate->interconnect_context->RecvTupleChunkFrom = RecvTupleChunkFromDeepMesh;
    estate->interconnect_context->RecvTupleChunkFromAny = RecvTupleChunkFromAnyDeepMesh;
    estate->interconnect_context->SendEos = SendEosDeepMesh;
    estate->interconnect_context->SendChunk = SendChunkDeepMesh;
    estate->interconnect_context->doSendStopMessage = doSendStopMessageDeepMesh;

    mySlice = (Slice *) list_nth(estate->interconnect_context->sliceTable->slices, LocallyExecutingSliceIndex(estate));

    Assert(estate->es_sliceTable &&
            IsA(mySlice, Slice) &&
            mySlice->sliceIndex == LocallyExecutingSliceIndex(estate));

    gp_interconnect_id = estate->interconnect_context->sliceTable->ic_instance_id;

    gp_set_monotonic_begin_time(&startTime);

    /* set deepmesh operation cancel checker to default one */
    dm_set_cancel_checker(defaultCancelCheck, estate->interconnect_context);

    /* Initiate outgoing connections. It will create sender endpoint */
    if (mySlice->parentIndex != -1) {
        sendingChunkTransportState = startOutgoingConnections(estate->interconnect_context, mySlice, &expectedTotalOutgoing);
    }

    /* Initiate incoming connections. It will create receiver endpoints*/
    startIncomingConnections(estate->interconnect_context, mySlice, &expectedTotalIncoming);

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
        ereport(DEBUG1, (errmsg("SetupInterconnect will activate "
                                "%d incoming, %d outgoing routes.  ",
                                expectedTotalIncoming, expectedTotalOutgoing)));

    /* After all endpoints are created, we should start to send registration msgs first to 
     * other peer receiver endpoints. This function will be blocked until all peer endpoints are up  */
    sendRegisterMessages(estate->interconnect_context, sendingChunkTransportState);

    /* Now we start to wait registration msgs from other senders(peer endpoints).
     * This function will be blocked until all peer endpoints are up and send registration msgs  */
    readRegisterMessages(estate->interconnect_context, mySlice);

    estate->interconnect_context->activated = true;
    if (gp_log_interconnect >= GPVARS_VERBOSITY_TERSE) {
        elapsed_ms = gp_get_elapsed_ms(&startTime);

        elog(DEBUG1, "SetupInterconnect+" UINT64_FORMAT "ms: Activated %d incoming, "
                 "%d outgoing routes.",
                 elapsed_ms, expectedTotalIncoming, expectedTotalOutgoing);
    }
} /* SetupInterconnect */

/* TeardownInterconnect() function is used to cleanup interconnect resources that
 * were allocated during SetupInterconnect().  This function should ALWAYS be
 * called after SetupInterconnect to avoid leaking resources (like sockets)
 * even if SetupInterconnect did not complete correctly.  As a result, this
 * function must complete successfully even if SetupInterconnect didn't.
 *
 * SetupInterconnect() always gets called under the ExecutorState MemoryContext.
 * This context is destroyed at the end of the query and all memory that gets
 * allocated under it is free'd.  We don't have have to worry about pfree() but
 * we definitely have to worry about socket resources.
 *
 * If forceEOS is set, we force end-of-stream notifications out any send-nodes,
 * and we wrap that send in a PG_TRY/CATCH block because the goal is to reduce
 * confusion (and if we're being shutdown abnormally anyhow, let's not start
 * adding errors!).
 */
void
TeardownDeepMeshInterconnect(ChunkTransportState *transportStates,
                        MotionLayerState *mlStates,
                        bool forceEOS, bool hasError)
{
    ListCell   *cell;
    ChunkTransportStateEntry *pEntry = NULL;
    int			i;
    Slice	   *mySlice;
    MotionConn *conn;

    if (transportStates->sliceTable == NULL) {
        elog(LOG, "TeardownDeepMeshInterconnect: missing slice table.");
        return;
    }

    /*
     * if we're already trying to clean up after an error -- don't allow
     * signals to interrupt us
    */
    if (forceEOS)
        HOLD_INTERRUPTS();

    mySlice = (Slice *) list_nth(transportStates->sliceTable->slices, transportStates->sliceId);

    /* Log the start of TeardownInterconnect. */
    if (gp_log_interconnect >= GPVARS_VERBOSITY_TERSE) {
        int	elevel = 0;

        if (forceEOS || !transportStates->activated) {
            if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
                elevel = LOG;
            else
                elevel = DEBUG1;
        }
        else if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            elevel = DEBUG4;
        }

        if (elevel)
            ereport(elevel, (errmsg("Interconnect seg%d slice%d cleanup state: "
                                    "%s; setup was %s",
                                    Gp_segment, mySlice->sliceIndex,
                                    forceEOS ? "force" : "normal",
                                    transportStates->activated ? "completed" : "exited")));

        /* if setup did not complete, log the slicetable */
        if (!transportStates->activated && gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            elog_node_display(DEBUG1, "local slice table", transportStates->sliceTable, true);
        }
    }

    /*
     * cleanup all of our Receiving Motion nodes, these get closed immediately
     * (the receiver know for real if they want to shut down -- they aren't
     * going to be processing any more data).
     */
    foreach(cell, mySlice->children) {
        Slice       *aSlice;
        int         childId = lfirst_int(cell);

        aSlice = (Slice *) list_nth(transportStates->sliceTable->slices, childId);
        getChunkTransportState(transportStates, aSlice->sliceIndex, &pEntry);

        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            elog(DEBUG1, "Interconnect closing connections from slice%d",
                aSlice->sliceIndex);
        }

        if( -1 != pEntry->dmEpHdlr) {
            /* TODO: should read all pending data before leave? */
            dm_ep_leave(pEntry->dmEpHdlr);
            if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
                ereport(DEBUG1, (errmsg("TeardownDeepMeshInterconnect sid %ld receiver ep %s leave.", 
                                     getDmSessId(),(char*)pEntry->dmLocalEp.id)));
            }
        }

        for (i = 0; i < pEntry->numConns; i++) {
            conn = pEntry->conns + i;

            /* free up the tuple remapper */
            if (conn->remapper) {
                DestroyTupleRemapper(conn->remapper);
                conn->remapper = NULL;
            }
        }
        removeChunkTransportState(transportStates, aSlice->sliceIndex);
        pfree(pEntry->conns);
    }

    /*
     * Now "normal" connections which made it through our peer-registration
     * step. With these we have to worry about "in-flight" data.
    */
    if (mySlice->parentIndex != -1) {
        /* cleanup a Sending motion node. */
        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            elog(DEBUG1, "Interconnect seg%d slice%d closing connections to slice%d",
                 Gp_segment, mySlice->sliceIndex, mySlice->parentIndex);
        }

        getChunkTransportState(transportStates, mySlice->sliceIndex, &pEntry);

        if (forceEOS && !hasError) {
            forceEosToPeers(mlStates, transportStates, mySlice->sliceIndex);
        }

        for (i = 0; i < pEntry->numConns; i++) {
            conn = pEntry->conns + i;
            /* free up the tuple remapper */
            if (conn->remapper) {
                DestroyTupleRemapper(conn->remapper);
                conn->remapper = NULL;
            }
        }
        
        if (!forceEOS) {
            /* TODO: we need to check that after dm_send() is successful, then close the 
             * src endpoint. Destionation should be able to read the sent data. If it's true
             * we don't need to wait for all peer receivers to leave the session */
        }

        /* close the sender endpoint */
        if( -1 != pEntry->dmEpHdlr) {
            dm_ep_leave(pEntry->dmEpHdlr);
            if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
                ereport(DEBUG1, (errmsg("TeardownDeepMeshInterconnect sid %ld ep %s leave.", 
                                     getDmSessId(),(char*)pEntry->dmLocalEp.id)));
            }
        }

        pEntry = removeChunkTransportState(transportStates, mySlice->sliceIndex);
        pfree(pEntry->conns);
    }

    /* Free all the resources allocated */
    transportStates->activated = false;
    transportStates->sliceTable = NULL;

    if (transportStates->states != NULL) {
        pfree(transportStates->states);
    }
    /* clear DeepMesh layer cancel checker */
    dm_set_cancel_checker(NULL, NULL);

    pfree(transportStates);

    if (forceEOS)
        RESUME_INTERRUPTS();

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
        elog(DEBUG1, "TeardownDeepMeshInterconnect successful");
}

static void
doSendStopMessageDeepMesh(ChunkTransportState *transportStates, int16 motNodeID)
{
    ChunkTransportStateEntry *pEntry = NULL;
    MotionConn *conn;
    int			i;
    char		m = 'S';

    getChunkTransportState(transportStates, motNodeID, &pEntry);
    Assert(pEntry);

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
        elog(DEBUG1, "Interconnect needs no more input from slice%d; notifying senders to stop.",
                     motNodeID);

    for (i = 0; i < pEntry->numConns; i++) {

        conn = pEntry->conns + i;
	    /* Note: we're only concerned with receivers here. */
        if (conn->dmEpHdlr >= 0 && mcsStarted == conn->state && conn->isReceiver) {

            if( 0 != dm_send(conn->dmEpHdlr, &conn->dmPeerEp, &m, sizeof(char), 0)) {
                /* Should always sent unless there is underling dm_agent error */
                 elog(LOG, "SendStopMessage: failed on dm_send. errno %d errmsg %s", dm_errno(), dm_errmsg());
            }

            /* will set the stillActive to false */
            DeregisterReadInterest(transportStates, motNodeID, i,
							   "no more input needed");
        }
    }
}

static TupleChunkListItem
RecvTupleChunkFromDeepMesh(ChunkTransportState *transportStates,
                      int16 motNodeID,
                      int16 srcRoute)
{
    ChunkTransportStateEntry *pEntry = NULL;
    MotionConn *conn;

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
        ereport(DEBUG1, (errmsg("RecvTupleChunkFromDeepMesh(motNodID=%d, srcRoute=%d)", motNodeID, srcRoute)));

    /* check em' */
    ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

    getChunkTransportState(transportStates, motNodeID, &pEntry);
    conn = pEntry->conns + srcRoute;

    readMsgFromConn(conn, transportStates);
    return RecvTupleChunk(conn, transportStates);
}

static TupleChunkListItem
RecvTupleChunkFromAnyDeepMesh(MotionLayerState *mlStates,
                         ChunkTransportState *transportStates,
                         int16 motNodeID,
                         int16 *srcRoute)
{
    ChunkTransportStateEntry *pEntry = NULL;
    TupleChunkListItem tcItem;

    getChunkTransportState(transportStates, motNodeID, &pEntry);
    Assert(pEntry);

    if(-1 == pEntry->dmEpHdlr)
        return NULL;

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        ereport(DEBUG1, (errmsg("Interconnect DeepMesh start to receive chunks from any sid %ld receiver ep %s.",
                    getDmSessId(), (char*)pEntry->dmLocalEp.id)));
    }

    bool  tryAgain; 
    do {
        const char *msg = NULL;
        int   msgSize = 0;
        int   index = 0;
        dm_ep_t srcEp;
        uint32  sizeInMsg = 0;

        /* Read full msg from the receiver endpoint. dm_recv() will be blocked until full msg is read
         * or is cancelled by interrupted/cancelFromQD, or connection error occurs.
         */
        memset(&srcEp, 0, sizeof(srcEp));

        dm_set_cancel_checker(readMsgCancelCheck, transportStates);
        msg = dm_recv(pEntry->dmEpHdlr, &srcEp, &msgSize, 0);
        dm_set_cancel_checker(defaultCancelCheck, transportStates);

        if( NULL == msg) {
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                   errmsg("Interconnect error reading any msg."),
                   errdetail("%s for sid %ld receiver ep %s errno %d errmsg %s: %m",
                      "dm_recv",
                      getDmSessId(),
                      (char*)pEntry->dmLocalEp.id,
                      dm_errno(),
                      dm_errmsg())));
        } else if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
            elog(DEBUG1, "RecvTupleChunkFromAnyDeepMesh dm_recv read msg size %d sid %ld receiver ep %s.",
                   msgSize, getDmSessId(), (char*)pEntry->dmLocalEp.id);
        }

        /* validate msg */
        if( msgSize < PACKET_HEADER_SIZE) {
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                    errmsg("Interconnect error read msg size less than packet head size ."),
                    errdetail("for sid %ld sender ep %s receiver ep %s msgsize %d: %m",
                      getDmSessId(),
                      (char*)srcEp.id,
                      (char*)pEntry->dmLocalEp.id,
                      msgSize)));
        }

        memcpy(&sizeInMsg, msg, sizeof(uint32));
        if (msgSize != sizeInMsg || msgSize > Gp_max_packet_size ) {
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                errmsg("Interconnect error read msg size is invalid ."),
                errdetail("for conn sid %ld sender ep %s receiver ep %s \
                      msgSize %d packMsgSize %d max_packet_size %d: %m",
                      getDmSessId(),
                      (char*)srcEp.id,
                      (char*)pEntry->dmLocalEp.id,
                      msgSize, sizeInMsg, Gp_max_packet_size)));
        }

        /* get connection based on srcEp. TODO: should we do hashmap for quick search */
        tryAgain = false;
        for(index = 0; index < pEntry->numConns; index++) {
            MotionConn *conn;

            conn = &pEntry->conns[index];
            if(0 == strcmp(conn->dmPeerEp.id, srcEp.id)) {
                if(!conn->stillActive) {
                    /* the connection is not active, discard the msg */
                    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
                        elog(DEBUG1, "RecvTupleChunkFromAnyDeepMesh receive chunks from inactive sender." \
                                     " sid %ld receiver ep %s source ep %s.",
                            getDmSessId(), (char*)pEntry->dmLocalEp.id, (char*)conn->dmPeerEp.id);
                    }
                    tryAgain = true;
                    break;
                }
                /* copy the data to pbuf */
                conn->recvBytes = msgSize;
                conn->msgSize = msgSize;
                memcpy(conn->pBuff, msg, msgSize);
                conn->msgPos = conn->pBuff;

                tcItem = RecvTupleChunk(conn, transportStates);
                *srcRoute = index;

                if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
                    elog(DEBUG1, "RecvTupleChunkFromAnyDeepMesh receive chunks from any sid %ld receiver ep %s source ep %s.",
                        getDmSessId(), (char*)pEntry->dmLocalEp.id, (char*)conn->dmPeerEp.id);
                }
                return tcItem;
            }
        }

        if(!tryAgain) {
            /* Received data from an unexpected sender . Should not happen, report error.*/
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                    errmsg("Interconnect error read msg from unexpected sender. "),
                    errdetail("sid %ld sender ep %s receiver ep %s : %m",
                      getDmSessId(),
                      (char*)srcEp.id,
                      (char*)pEntry->dmLocalEp.id)));
        }
    } while(tryAgain);

    // should never reach it.
    return NULL;
}

/* See ml_ipc.h */
static void
SendEosDeepMesh(MotionLayerState *mlStates,
		   ChunkTransportState *transportStates,
           int motNodeID,
           TupleChunkListItem tcItem)
{
    ChunkTransportStateEntry *pEntry = NULL;
    MotionConn *conn;
    int i;

    if (!transportStates) {
        elog(FATAL, "SendEosDeepMesh: missing interconnect context.");
    }
    else if (!transportStates->activated && !transportStates->teardownActive) {
        elog(FATAL, "SendEosDeepMesh: context and teardown inactive.");
    }

    getChunkTransportState(transportStates, motNodeID, &pEntry);
    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        elog(DEBUG1, "Interconnect seg%d slice%d sending end-of-stream to slice%d",
            Gp_segment, motNodeID, pEntry->recvSlice->sliceIndex);
    }

    /*
     * we want to add our tcItem onto each of the outgoing buffers -- this is
     * guaranteed to leave things in a state where a flush is *required*.
     */
    doBroadcast(mlStates, transportStates, pEntry, tcItem, NULL);

    /* now flush all of the buffers. */
    for (i = 0; i < pEntry->numConns; i++) {
        conn = pEntry->conns + i;

        if (conn->dmEpHdlr >= 0 && conn->state == mcsStarted) {
            flushBufferDeepMesh(mlStates, transportStates, pEntry, conn, motNodeID);
        }

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        elog(DEBUG1, "SendEosDeepMesh() Leaving");
    }

    return;
}

/* Send all buffered data in the MotionConn to peer EP */
static bool
flushBufferDeepMesh(MotionLayerState *mlStates, ChunkTransportState *transportStates,
            ChunkTransportStateEntry *pEntry, MotionConn *conn, int16 motionId)
{
    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        struct timeval snapTime;

        gettimeofday(&snapTime, NULL);
        elog(DEBUG1, "----sending chunk @%s.%d time is %d.%d",
            __FILE__, __LINE__, (int) snapTime.tv_sec, (int) snapTime.tv_usec);
    }
    
    /* first set header length */
    *(uint32 *) conn->pBuff = conn->msgSize;

    /* send data to remote EP with flag DM_FLAG_SEND_CHECK_STOP which will 
     * block the sender until interrupted or a msg is received from receiver 
     * (we can assume that's stop msg from receiver)
     */
    if(0 != dm_send(conn->dmEpHdlr, &conn->dmPeerEp, (char*)conn->pBuff, conn->msgSize, DM_FLAG_SEND_CHECK_STOP)) {
        if(dm_errno() == DM_ERR_STOP_MSG) {
            conn->stillActive = false;
            return false;
        } else {
            /* if we got an error in teardown, ignore it: treat it as a stop message */
            if (transportStates->teardownActive) {
                conn->stillActive = false;
                return false;
            }
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                    errmsg("Interconnect error flush buffer DeepMesh failed"),
                    errdetail("error during dm_send() call (errno %d errmsg %s)."
                              "For sid %ld sender ep %s receiver ep %s",
                               dm_errno(), dm_errmsg(),
                               getDmSessId(),
                               (char*)conn->dmLocalEp.id,
                               (char*)conn->dmPeerEp.id)));
        }
    }

    conn->tupleCount = 0;
    conn->msgSize = PACKET_HEADER_SIZE;

    return true;
}

/* The Function sendChunk() is used to send a tcItem to a single
 * destination. Tuples often are *very small* we aggregate in our
 * local buffer before sending into the kernel.
 *
 * PARAMETERS
 *	 conn - MotionConn that the tcItem is to be sent to.
 *	 tcItem - message to be sent.
 *	 motionId - Node Motion Id.
 */
static bool
SendChunkDeepMesh(MotionLayerState *mlStates, ChunkTransportState *transportStates, 
            ChunkTransportStateEntry *pEntry, MotionConn *conn, TupleChunkListItem tcItem, int16 motionId)
{
    int length = TYPEALIGN(TUPLE_CHUNK_ALIGN, tcItem->chunk_length);

    Assert(conn->msgSize > 0);

    if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
        elog(DEBUG1, "sendChunk: msgSize %d this chunk length %d", conn->msgSize, tcItem->chunk_length);
    }

    if (conn->msgSize + length > Gp_max_packet_size) {
        if (!flushBufferDeepMesh(mlStates, transportStates, pEntry, conn, motionId))
            return false;
    }
    memcpy(conn->pBuff + conn->msgSize, tcItem->chunk_data, tcItem->chunk_length);
    conn->msgSize += length;
    conn->tupleCount++;

    return true;
}
