#ifndef DEEPMESH_H
#define DEEPMESH_H

#include <stdint.h>

#define DM_ERR_TIMEOUT           1
#define DM_ERR_NOTCONNECTED      2
#define DM_ERR_ALREADY_CONNECTED 3
#define DM_ERR_SOCK_CREATE       4 
#define DM_ERR_SOCK_CONN         5 
#define DM_ERR_CONN_TEARDOWN     6 
#define DM_ERR_CONN_READ         7 
#define DM_ERR_CONN_WRITE        8 

#define DM_ERR_SESS_DUP          20   // duplicated session id
#define DM_ERR_SESS_NOTEXIST     21   // session id does not exist
#define DM_ERR_SESS_DESTROY      22   // session is destroyed by remote

#define DM_ERR_EP_DUP            30   // duplicated endpoint id
#define DM_ERR_LOCAL_EP_NOTEXIST 31   // local endpoint does not exist
#define DM_ERR_PEER_EP_NOTEXIST  32   // peer endpoint does not exist
#define DM_ERR_EP_LEAVE          33   // the endpoint leaves
#define DM_ERR_EP_MAXNUM         34   // reach maximum ep number per connection

#define DM_ERR_BAD_MSG           40   // the message received is bad
#define DM_ERR_NOMEMORY          41   // no enough memory
#define DM_ERR_STOP_MSG          42   // sender receives stop msg from peer

// When this flag is set, both send/recv will be blocked until peer EP is joined 
// and data is sent or data is received.
#define DM_FLAG_WAIT_EP_JOIN     0x0001 

// When this flag is set, dm_send will check if any msg recevied from the remote peer.
// If there is msg available, dm_send will return.
#define DM_FLAG_SEND_CHECK_STOP    0x0002

#define INVALID_HANDLE      -1
typedef int dm_handle_t;

typedef struct dm_ep_t dm_ep_t;
struct dm_ep_t {
    char id[32];
};

#define DM_CONFIG_ADD           0x01
#define DM_CONFIG_REMOVE        0x02
typedef struct dm_agent_cfg_t {
    uint32_t         addr;       // agent address
    uint16_t         port;       // agent listen port
    uint8_t          action;     // add or remove
} dm_agent_cfg_t;

typedef void (*dm_cancel_check_func_pt)(void *param);

/**
 *  Retrieve last error number
 */
int dm_errno();

/**
 *  Retrieve last error message.
 */
const char* dm_errmsg();

/**
 *  Connect to the local deepmesh agent. Returns 0 on success, -1 otherwise.
 */
int dm_connect(const char* path);

/**
 *  Disconnect from the local deepmesh agent. 
 */
void dm_disconnect();

/**
 *  Create a session. Returns 0 on success, -1 otherwise.
 */
int dm_sess_create(uint64_t sid);

/**
 *  Destroy a session. All mailboxes are dropped automatically.
 */
void dm_sess_destroy(uint64_t sid);

/**
 *  Join a session and provide my peer id unique to the session.
 *  Returns a handle descriptor on success, INVALID_HANDLE otherwise.
 */
dm_handle_t dm_ep_join(uint64_t sid, const dm_ep_t* myid);

/**
 *  Leave the session. My mailbox will be destroyed.
 */
void dm_ep_leave(dm_handle_t h);

/**
 *  Send a message. May block if receiver is not ready or 
 *  the dst is not (yet) known.
 *  Returns 0 on success, -1 otherwise.
 */
int dm_send(dm_handle_t h, const dm_ep_t* dst, const char* msg, int msgsz, int flag);

 /**
  *  Block and wait for a message until interrupted or error occurs
  *  Returns a pointer to the message on success, NULL otherwise.
  *
  *  If *src = '\0' , then we read a message from any source.
  *  The out param src will be filled with ID of the sender.
  *  otherwise, we read the message from the particular source. If source
  *  endpoint has not joined yet, the function will be blocked until source
  *  ep is joined and send data.
  *  The out param msgsz will be filled with the size of the message.
  *  The returned message is valid until the next time dm_recv(), dm_recv_any()
  *  or dm_ep_leave(), dm_sess_destroy(), dm_disconnt() are called.
  * 
  *  flag can be DM_FLAG_WAIT_EP_JOIN or DM_FLAG_RECV_NOBLOCK
  */
const char* dm_recv(dm_handle_t h, dm_ep_t* src, int* msgsz, int flag);

/**
 * set cancel check function which is called during any blocking functions
 * to allow cancelling of the blocked function by interrupt etc.
 */
void dm_set_cancel_checker(dm_cancel_check_func_pt checker, void *param);

/**
  * configure the agent.
  * return 0 on success, -1 otherwise
  */
int dm_config_agent(dm_agent_cfg_t *configs, uint16_t cfg_num);

#endif /* DEEPMESH_H */
