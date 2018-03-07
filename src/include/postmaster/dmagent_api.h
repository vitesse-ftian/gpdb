#ifndef DMAGENT_API_H
#define DMAGENT_API_H

typedef struct dm_agent_addr_port_t {
    uint32_t         addr;       // remote agent address
    uint16_t         port;       // remote agent listen port
} dm_agent_addr_port_t;

/**
  * start deepmesh agent
  * return 0 if success, -1 otherwise
  */
int dm_agent_start(char *cfg_fn,
                   char *log_path,
                   bool is_master,
                   dm_agent_addr_port_t *agents,
                   int  agent_count,
                   uint32_t my_addr,
                   uint32_t master_addr);

/**
  * set to 1 if we need to shutdown DM agent.
  */
extern int dmagent_should_shutdown;
extern int dmagent_got_usr1;
extern int dmagent_got_sighup;

#endif /* DMAGENT_API_H */
