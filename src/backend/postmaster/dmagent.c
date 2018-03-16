/*-------------------------------------------------------------------------
 *
 * dmagent.c
 *
 * The dmagent is started by the postmaster at the beginning of the startup subprocess
 * It remains alive until the postmaster commands it to terminate.
 * Normal termination is by SIGTERM, which instructs the dmagent to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the dmagent will
 * simply abort and exit on SIGQUIT.
 *
 * If the dmagent exits unexpectedly, the postmaster treats that the same
 * as a backend crash: the connection to dmagent needs to be established. so remaining
 * backends should be killed by SIGQUIT and then a recovery cycle started.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "libpq/pqsignal.h"
#include "postmaster/dmagent_api.h"
#include "postmaster/dmagent.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"

static bool DeepMeshAgentStartedByMe = false;

#define DMAGENT_LOCK_FILE	"/tmp/dmagent_pid_lock"

/* Signal handlers */
static void dmagent_quickdie(SIGNAL_ARGS);
static void dmagent_shutdown(SIGNAL_ARGS);
static void dmagent_usr1(SIGNAL_ARGS);
static void dmagent_sighup(SIGNAL_ARGS);

static void read_agent_cfgs(dm_agent_addr_port_t *agents, int *agent_num);

extern char *Log_directory;
extern bool Gp_entry_postmaster;

static char *knownDatabase = "postgres";

/*
 * Main entry point for deepmesh agent process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void
DeepMeshAgentMain(int argc, char *argv[])
{
	dm_agent_addr_port_t dmagents[1024];
	int dmagent_num = 0;
	bool dmagent_ismaster = false; 
	sigjmp_buf      local_sigjmp_buf; 
	char       *fullpath;

	IsUnderPostmaster = true;   /* we are a postmaster subprocess now */
	MyProcPid = getpid();               /* reset MyProcPid */
	MyStartTime = time(NULL);       /* set our start time in case we call elog */

	/* Stay away from PMChildSlot */
	MyPMChildSlot = -1;
 
	if (Gp_entry_postmaster && Gp_role == GP_ROLE_DISPATCH) {
		init_ps_display("master deepmesh agent process", "", "", "");
		dmagent_ismaster = true;
	}
	else {
		init_ps_display("deepmesh agent process", "", "", "");
	}

	SetProcessingMode(InitProcessing);

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 */
	pqsignal(SIGHUP, dmagent_sighup);    /* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, dmagent_shutdown);
	pqsignal(SIGQUIT, dmagent_quickdie);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, dmagent_usr1);  /* request log current internal status*/
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

        /* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
        sigdelset(&BlockSig, SIGQUIT);
#else
        BlockSig &= ~(sigmask(SIGQUIT));
#endif
	/* Early initialization */ 
	BaseInit();

 	/* See InitPostgres()... */ 
	InitProcess(); 
	InitBufferPoolBackend(); 
	InitXLOGAccess(); 

	SetProcessingMode(NormalProcessing); 

	/* 
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0) { 
		/* Prevents interrupts while cleaning up */ 
		HOLD_INTERRUPTS(); 

		/* Report the error to the server log */ 
		EmitErrorReport();

		/*
		 * We can now go away.  Note that because we'll call InitProcess, a
		 * callback will be registered to do ProcKill, which will clean up
		 * necessary state.
		 */ 
		proc_exit(0); 
	}

	/* We can now handle ereport(ERROR) */ 
	PG_exception_stack = &local_sigjmp_buf; 
	PG_SETMASK(&UnBlockSig);

	/* 
	 * Create a resource owner to keep track of our resources (currently only 
	 * buffer pins). 
	 */
        CurrentResourceOwner = ResourceOwnerCreate(NULL, "DmAgent");

	/*
	 * Add my PGPROC struct to the ProcArray.
	 * 
	 * Once I have done this, I am visible to other backends!
	 */ 
	InitProcessPhase2();

	/* 
	 * Initialize my entry in the shared-invalidation manager's array of
	 * per-backend data.
	 *
	 * Sets up MyBackendId, a unique backend identifier. 
	 */
	MyBackendId = InvalidBackendId;
	
	SharedInvalBackendInit(false);

	if (MyBackendId > MaxBackends || MyBackendId <= 0) 
		elog(FATAL, "bad backend id: %d", MyBackendId);

        /*
         * bufmgr needs another initialization call too
         */
        InitBufferPoolBackend();

        /* heap access requires the rel-cache */
        RelationCacheInitialize();
        InitCatalogCache();

        /*
         * It's now possible to do real access to the system catalogs.
         *
         * Load relcache entries for the system catalogs.  This must create at
         * least the minimum set of "nailed-in" cache entries.
         */
        RelationCacheInitializePhase2();

        /*
         * In order to access the catalog, we need a database, and a
         * tablespace; our access to the heap is going to be slightly
         * limited, so we'll just use some defaults.
         */
        if (!FindMyDatabase(knownDatabase, &MyDatabaseId, &MyDatabaseTableSpace))
                ereport(FATAL,
                                (errcode(ERRCODE_UNDEFINED_DATABASE),
                                 errmsg("database \"%s\" does not exit", knownDatabase)));

        /* Now we can mark our PGPROC entry with the database ID */
        /* (We assume this is an atomic store so no lock is needed) */
        MyProc->databaseId = MyDatabaseId;

        fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

        SetDatabasePath(fullpath);

        RelationCacheInitializePhase3();

	if (Gp_entry_postmaster && Gp_role == GP_ROLE_DISPATCH) {
		read_agent_cfgs(dmagents, &dmagent_num);
	}

	/* release the resource after reading cfgs from shared memeory*/
	ResourceOwnerRelease(CurrentResourceOwner,
				RESOURCE_RELEASE_BEFORE_LOCKS, false, true);

	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Drop our connection to postmaster's shared memory */
	PGSharedMemoryDetach();

	/* start the real work */
	int ret = dm_agent_start("dmagent.conf", 
					Log_directory,
					dmagent_ismaster,
					dmagents,
					dmagent_num,
					gp_master_address);
	exit(ret);
}

/*
 * The start function of deepmesh agent invoked by postmaster
 * The function will check if DM agent has been started in this physical 
 * server first by checking dmagent_pid_lock file under /tmp directory.
 * If it's already started by other postmaster, ShouldStartDeepMeshAgent is set to false
 * dmagent_pid_lock should be deleted when dmagent process exits
 */
int
DeepMeshAgent_Start()
{
	pid_t	dmagent_pid;

	if( INTERCONNECT_TYPE_DEEPMESH != Gp_interconnect_type)
		return 0;

	/* lock file check */
	if(!DeepMeshAgentStartedByMe) {
		if (!(Gp_entry_postmaster && Gp_role == GP_ROLE_DISPATCH)){
			if('\0' == gp_master_address[0]) {
				/* This segment is in same host as master */
				return 0;
			}
		}

		/* Did not start DM agent. Try to do it by checking lock file */
		int fd = open(DMAGENT_LOCK_FILE, O_RDWR | O_CREAT | O_EXCL, 0600);
		if(fd < 0 && EEXIST == errno) {
			if(gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
				/* read the pid which starts the DM agent and log it */
				fd = open(DMAGENT_LOCK_FILE, O_RDONLY);
				if(fd >=0) {
					char buffer[100];
					int count = read(fd, buffer, 100);
					if(count > 0) {
						char *endptr;
						long pid = strtol(buffer, &endptr, 10);
						elog(DEBUG1, "Another postmaster %ld has already started deepmesh agent", pid);
					}
					close(fd);
				}
			}
			return 0;

		} else if (fd < 0) {
			elog(FATAL, "can not open %s for write. reason %s", DMAGENT_LOCK_FILE, strerror(errno));
			return 0;
		}

		/* success, in postmaster. write my PID to dmagent_pid_lock */
		char buffer[100];
		sprintf(buffer, "%d", getpid());
 		if (write(fd, buffer, strlen(buffer)) != strlen(buffer)) {
			int	save_errno = errno;

			close(fd);
			unlink(DMAGENT_LOCK_FILE);
			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;
			ereport(FATAL, (errcode_for_file_access(),
				errmsg("could not write deepmesh agent lock file \"%s\": %m", DMAGENT_LOCK_FILE)));
		}
		DeepMeshAgentStartedByMe = true;

	}

#ifdef EXEC_BACKEND
	switch ((dmagent_pid = dmagent_forkexec()))
#else
	switch ((dmagent_pid = fork_process()))
#endif
	{
		case -1:
			ereport(FATAL, (errmsg("could not fork deepmesh agent: %m"))); 
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(true);

			/* do the work */
			DeepMeshAgentMain(0, NULL);
			break;
#endif

		default:
			/* success, in postmaster. return pid */
			elog(LOG, "deepmesh agent with pid %d is started", dmagent_pid);
			return dmagent_pid;
	}

	/* should never reach here */
	return 0;
}

/* This function is invoked by PostManager when Postmanager exits.
 * It is to cleanup the dmagent lock file
 */
void DeepMeshAgent_Cleanup()
{
	if( DeepMeshAgentStartedByMe) {
		unlink(DMAGENT_LOCK_FILE);
	}
}

static void read_agent_cfgs(dm_agent_addr_port_t *agents, int *agent_count)
{
	/* Read DB schema */
	for(;;) {
		CdbComponentDatabases *dbs = getCdbComponentDatabases(); 

		if (dbs == NULL || dbs->total_entry_dbs <= 0 || dbs->total_segment_dbs <= 0) {
			elog(DEBUG1, "schema not populated while building deepmesh agent config");
			sleep(3);
			continue;
		} 

		for( int i=0; i < dbs->total_entry_dbs; i++) {
			bool found = false;

			for(int j=0; j<*agent_count; j++) {
				if(strncpy(agents[j].addr, dbs->entry_db_info[i].hostip, INET6_ADDRSTRLEN) == 0) {
					found = true;
					break;
				}
			}
			if (!found) {
				elog(DEBUG1, "Found unique entry db %d dbid %d segindex %d role %c hostname %s hostip %s, hostSegs %d",
					i, dbs->entry_db_info[i].dbid,
					dbs->entry_db_info[i].segindex,
					dbs->entry_db_info[i].role,
					dbs->entry_db_info[i].hostname, 
					dbs->entry_db_info[i].hostip, 
					dbs->entry_db_info[i].hostSegs);
				strncpy(agents[*agent_count].addr, dbs->entry_db_info[i].hostip, INET6_ADDRSTRLEN);
				agents[*agent_count].port = 3333;
				(*agent_count)++;
			}
		}

		for( int i=0; i < dbs->total_segment_dbs; i++) { 
			bool found = false;

			for(int j=0; j<*agent_count; j++) {
				if(strncpy(agents[j].addr, dbs->segment_db_info[i].hostip, INET6_ADDRSTRLEN) == 0) {
					found = true;
					break;
				}
			}
			if (!found) {
				elog(DEBUG1, "Found unique segment %d dbid %d segindex %d role %c hostname %s hostip %s, hostSegs %d",
					i, dbs->segment_db_info[i].dbid,
					dbs->segment_db_info[i].segindex,
					dbs->segment_db_info[i].role,
					dbs->segment_db_info[i].hostname, 
					dbs->segment_db_info[i].hostip, 
					dbs->segment_db_info[i].hostSegs);
				strncpy(agents[*agent_count].addr, dbs->segment_db_info[i].hostip, INET6_ADDRSTRLEN);
				agents[*agent_count].port = 3333;
				(*agent_count)++;
			}
		}

		if(gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG) {
			for(int i=0; i<*agent_count; i++) {
				elog(DEBUG1, "Constructed Deepmesh agent %i: host ip %s port %d",
					i, agents[i].addr, agents[i].port);
			}
		}

		return;
	}

	// should never be reached.
	return;
}

#ifdef EXEC_BACKEND
/*
 * dmagent_forkexec() -
 *
 * Format up the arglist for, then fork and exec, a deepmesh agent process
 */
static pid_t
dmagent_forkexec(void)
{
	char	*av[10];
	int	ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkdmagent";
	av[ac++] = NULL;                 

	return postmaster_forkexec(ac, av);
}
#endif

/*
 * wal_quickdie() occurs when signalled SIGQUIT by the postmaster.
 * we stop immediately when receives this signal
 */
static void
dmagent_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);

	dmagent_should_shutdown = true;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
dmagent_sighup(SIGNAL_ARGS)
{
	dmagent_got_sighup = true;
}

/* SIGTERM: set flag to exit normally */
static void
dmagent_shutdown(SIGNAL_ARGS)
{
	dmagent_should_shutdown = true;
}

/* SIGUSR1: ask agent to log internal data structure */
static void
dmagent_usr1(SIGNAL_ARGS)
{
	dmagent_got_usr1 = true;
}
