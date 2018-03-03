/*-------------------------------------------------------------------------
 *
 * dmagent.h
 *	  Exports from postmaster/dmagent.c.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _DMAGENT_H
#define _DMAGENT_H

#ifdef EXEC_BACKEND
extern void DeepMeshAgentMain(int argc, char *argv[]);
#endif

extern int  DeepMeshAgent_Start(void);
extern void DeepMeshAgent_Cleanup(void);

#endif   /* _DMAGENT_H */
