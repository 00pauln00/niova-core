/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef _NIOVA_ENV_H_
#define _NIOVA_ENV_H_ 1

#include "init.h"

typedef void env_cb_ctx_t;

enum niova_env_var_type
{
    NIOVA_ENV_VAR_TYPE_NONE,
    NIOVA_ENV_VAR_TYPE_STRING,
    NIOVA_ENV_VAR_TYPE_LONG,
    NIOVA_ENV_VAR_TYPE_OTHER,
    //NIOVA_ENV_VAR_TYPE_PATHNAME
    //XXx needed for checking inotify paths for absoluteness
} PACKED;

enum niova_env_subsystem
{
    NIOVA_ENV_SUBSYSTEM_LOG,
    NIOVA_ENV_SUBSYSTEM_AIO,
    NIOVA_ENV_SUBSYSTEM_CTL_INTERFACE,
    NIOVA_ENV_SUBSYSTEM_RAFT_CLIENT,
    NIOVA_ENV_SUBSYSTEM_WATCHDOG,
    NIOVA_ENV_SUBSYSTEM_CTL_SVC,
    NIOVA_ENV_SUBSYSTEM_NET,
} PACKED;

enum niova_env_var_num
{
    NIOVA_ENV_VAR_alloc_log_level,
    NIOVA_ENV_VAR_ctl_interface_init_path,
    NIOVA_ENV_VAR_epoll_mgr_nevents,
    NIOVA_ENV_VAR_inotify_base_path,
    NIOVA_ENV_VAR_inotify_path,
    NIOVA_ENV_VAR_local_ctl_svc_dir,
    NIOVA_ENV_VAR_log_level,
    NIOVA_ENV_VAR_num_aio_events,
    NIOVA_ENV_VAR_raft_client_request_timeout,
    NIOVA_ENV_VAR_uuid,
    NIOVA_ENV_VAR_watchdog_disable,
    NIOVA_ENV_VAR_watchdog_frequency,
    NIOVA_ENV_VAR_watchdog_stall_cnt,
    NIOVA_ENV_VAR_tcp_disable,
    NIOVA_ENV_VAR__MAX,
    NIOVA_ENV_VAR__MIN = NIOVA_ENV_VAR_alloc_log_level,
} PACKED;

struct niova_env_var
{
    const char              *nev_name;
    const char              *nev_string;
    long long                nev_min;
    long long                nev_default;
    long long                nev_max;
    long long                nev_long_value;
    bool                     nev_present;
    enum niova_env_var_type  nev_type;
    enum niova_env_subsystem nev_subsystem;
    enum niova_env_var_num   nev_var_num;
    int                      nev_rc;
    void                     (*nev_cb)(const struct niova_env_var *);
};

const struct niova_env_var *
env_get(enum niova_env_var_num ev);

#endif
