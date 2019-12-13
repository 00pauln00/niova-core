/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef _NIOVA_ENV_H_
#define _NIOVA_ENV_H_ 1

#include "init.h"

enum niova_env_var_type
{
    NIOVA_ENV_VAR_TYPE_NONE,
    NIOVA_ENV_VAR_TYPE_STRING,
    NIOVA_ENV_VAR_TYPE_LONG,
    NIOVA_ENV_VAR_TYPE_OTHER,
};

enum niova_env_subsystem
{
    NIOVA_ENV_SUBSYSTEM_LOG,
    NIOVA_ENV_SUBSYSTEM_AIO,
    NIOVA_ENV_SUBSYSTEM_INOTIFY,
    NIOVA_ENV_SUBSYSTEM_WATCHDOG,
    NIOVA_ENV_SUBSYSTEM_CTL_SVC,
};

enum niova_env_var_num
{
    NIOVA_ENV_VAR_MIN                = 0,
    NIOVA_ENV_VAR_log_level          = 0,
    NIOVA_ENV_VAR_num_aio_events     = 1,
    NIOVA_ENV_VAR_inotify_path       = 2,
    NIOVA_ENV_VAR_watchdog_disable   = 3,
    NIOVA_ENV_VAR_watchdog_frequency = 4,
    NIOVA_ENV_VAR_watchdog_stall_cnt = 5,
    NIOVA_ENV_VAR_alloc_log_level    = 6,
    NIOVA_ENV_VAR_epoll_mgr_nevents  = 7,
    NIOVA_ENV_VAR_local_ctl_svc_dir  = 8,
    NIOVA_ENV_VAR_MAX                = 9,
};

struct niova_env_var
{
    const char              *nev_name;
    const char              *nev_string;
    enum niova_env_var_type  nev_type;
    enum niova_env_subsystem nev_subsystem;
    enum niova_env_var_num   nev_var_num;
    long long                nev_min;
    long long                nev_default;
    long long                nev_max;
    long long                nev_long_value;
    bool                     nev_present;
    int                      nev_rc;
    void                   (*nev_cb)(const struct niova_env_var *);
};

const struct niova_env_var *
env_get(enum niova_env_var_num ev);

init_ctx_t
env_init(void)
    __attribute__ ((constructor (ENV_VAR_SUBSYS_CTOR_PRIORITY)));

destroy_ctx_t
env_destroy(void)
    __attribute__ ((destructor (ENV_VAR_SUBSYS_CTOR_PRIORITY)));

#endif
