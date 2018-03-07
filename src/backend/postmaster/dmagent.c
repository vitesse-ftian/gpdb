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
#include "utils/guc.h"
#include "cdb/cdbvars.h"

static bool DeepMeshAgentStartedByMe = false;

#define DMAGENT_LOCK_FILE	"/tmp/dmagent_pid_lock"

static dm_agent_addr_port_t dmagents[1024];
static int dmagent_num = 0;
static uint32_t dmagent_myaddr = 0;
static uint32_t dmagent_master_addr = 0;
static bool dmagent_ismaster = false;


/* Signal handlers */
static void dmagent_quickdie(SIGNAL_ARGS);
static void dmagent_shutdown(SIGNAL_ARGS);
static void dmagent_usr1(SIGNAL_ARGS);
static void dmagent_sighup(SIGNAL_ARGS);

extern char *Log_directory;
extern bool Gp_entry_postmaster;

/*
 * Main entry point for deepmesh agent process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void
DeepMeshAgentMain(int argc, char *argv[])
{
	IsUnderPostmaster = true;   /* we are a postmaster subprocess now */
	MyProcPid = getpid();               /* reset MyProcPid */
	MyStartTime = time(NULL);       /* set our start time in case we call elog */

	if (Gp_entry_postmaster && Gp_role == GP_ROLE_DISPATCH)
		init_ps_display("master deepmesh agent process", "", "", "");
	else
		init_ps_display("deepmesh agent process", "", "", "");

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

	PG_SETMASK(&UnBlockSig);

	/* start the real work */
	proc_exit(dm_agent_start("dmagent.conf", 
					Log_directory,
					dmagent_ismaster,
					dmagents,
					dmagent_num,
					dmagent_myaddr,
					dmagent_master_addr));
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

	/*TODO: get myaddr, master_addr */

	if (!(Gp_entry_postmaster && Gp_role == GP_ROLE_DISPATCH)){
		/* Do not start deepmesh agent for segments in the same host as master */
		if(dmagent_myaddr == dmagent_master_addr)
			return 0;
		
	}

	/* lock file check */
	if(!DeepMeshAgentStartedByMe) {
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
						elog(DEBUG5, "Another postmaster %ld has already started deepmesh agent", pid);
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

		if (Gp_entry_postmaster && Gp_role == GP_ROLE_DISPATCH){
			/* TODO: In master postgres, read all segments info */
			dmagent_ismaster = true;
		}
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

			/* Lose the postmaster's on-exit routines */
			on_exit_reset();

			/* Drop our connection to postmaster's shared memory, as well */
			PGSharedMemoryDetach();

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
