/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <stdlib.h>

#include "ctor.h"
#include "env.h"

#include "log.h"
#include "niosd_io.h"
#include "watchdog.h"
#include "epoll_mgr.h"
#include "alloc.h"
#include "ctl_svc.h"
#include "udp.h"

static bool niovaEnvVarsSubsysInit = false;

static struct niova_env_var niovaEnvVars[] = {
    {
        .nev_name      = "NIOVA_LOG_LEVEL",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_LOG,
        .nev_var_num   = NIOVA_ENV_VAR_log_level,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_default   = LL_WARN,
        .nev_min       = 0,
        .nev_max       = LL_MAX,
        .nev_present   = false,
    },
    {
        .nev_name      = "NIOVA_NUM_AIO_EVENTS",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_AIO,
        .nev_var_num   = NIOVA_ENV_VAR_num_aio_events,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_default   = NIOSD_DEFAULT_AIO_EVENTS,
        .nev_min       = NIOSD_MIN_AIO_EVENTS,
        .nev_max       = NIOSD_MAX_AIO_EVENTS,
        .nev_present   = false,
    },
    {
        .nev_name      = "NIOVA_INOTIFY_PATH",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_INOTIFY,
        .nev_var_num   = NIOVA_ENV_VAR_inotify_path,
        .nev_type      = NIOVA_ENV_VAR_TYPE_STRING,
        .nev_present   = false,
    },
    {
        .nev_name      = "NIOVA_WATCHDOG_DISABLE",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_WATCHDOG,
        .nev_var_num   = NIOVA_ENV_VAR_watchdog_disable,
        .nev_type      = NIOVA_ENV_VAR_TYPE_NONE,
        .nev_present   = false,
    },
    {
        .nev_name      = "NIOVA_WATCHDOG_FREQUENCY",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_WATCHDOG,
        .nev_var_num   = NIOVA_ENV_VAR_watchdog_frequency,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_default   = WATCHDOG_DEFAULT_FREQUENCY,
        .nev_min       = WATCHDOG_MIN_FREQUENCY,
        .nev_max       = WATCHDOG_MAX_FREQUENCY,
        .nev_present   = false,
    },
    {
        .nev_name      = "NIOVA_WATCHDOG_STALL_CNT",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_WATCHDOG,
        .nev_var_num   = NIOVA_ENV_VAR_watchdog_stall_cnt,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_default   = WATCHDOG_DEFAULT_STALL_CNT,
        .nev_min       = WATCHDOG_MIN_STALL_CNT,
        .nev_max       = WATCHDOG_MAX_STALL_CNT,
        .nev_present   = false,
    },
    {
        .nev_name      = "NIOVA_ALLOC_LOG_LEVEL",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_LOG,
        .nev_var_num   = NIOVA_ENV_VAR_alloc_log_level,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_default   = LL_WARN,
        .nev_min       = 0,
        .nev_max       = LL_MAX,
        .nev_present   = false,
        .nev_cb        = alloc_env_var_cb,
    },
    {
        .nev_name      = "NIOVA_EPOLL_MGR_NEVENTS",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_AIO,
        .nev_var_num   = NIOVA_ENV_VAR_epoll_mgr_nevents,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_default   = EPOLL_MGR_DEF_EVENTS,
        .nev_min       = EPOLL_MGR_MIN_EVENTS,
        .nev_max       = EPOLL_MGR_MAX_EVENTS,
        .nev_present   = false,
        .nev_cb        = epoll_mgr_env_var_cb,
    },
    {
        .nev_name      = "NIOVA_LOCAL_CTL_SVC_DIR",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_CTL_SVC,
        .nev_var_num   = NIOVA_ENV_VAR_local_ctl_svc_dir,
        .nev_type      = NIOVA_ENV_VAR_TYPE_STRING,
        .nev_present   = false,
        .nev_cb        = ctl_svc_set_local_dir,
    },
    {
        .nev_name      = "NIOVA_UDP_PORT",
        .nev_subsystem = NIOVA_ENV_SUBSYSTEM_CTL_SVC,
        .nev_var_num   = NIOVA_ENV_VAR_local_ctl_svc_dir,
        .nev_type      = NIOVA_ENV_VAR_TYPE_LONG,
        .nev_present   = false,
        .nev_min       = NIOVA_MIN_UDP_PORT,
        .nev_max       = NIOVA_MAX_UDP_PORT,
        .nev_default   = NIOVA_DEFAULT_UDP_PORT,
        .nev_cb        = udp_env_set_default_port,
    },
};

static void
env_parse_long(const char *ev_string, struct niova_env_var *nev)
{
    if (!ev_string || !nev)
        return;

    nev->nev_long_value = strtoll(ev_string, NULL, 10);

    if (nev->nev_long_value == LLONG_MIN || nev->nev_long_value == LLONG_MAX)
    {
        nev->nev_rc = -errno;
        nev->nev_long_value = -1;

        SIMPLE_LOG_MSG(LL_WARN, "env-var %s has invalid value (%s): %s",
                       nev->nev_name, ev_string, strerror(-nev->nev_rc));

        return;
    }

    if (nev->nev_long_value > nev->nev_max)
    {
        nev->nev_rc = -ERANGE;

        SIMPLE_LOG_MSG(LL_WARN,
                       "env-var %s exceeds the max value (%lld), applying max",
                       nev->nev_name, nev->nev_max);

        nev->nev_long_value = nev->nev_max;
    }
    else if (nev->nev_long_value < nev->nev_min)
    {
        nev->nev_rc = -ERANGE;

        SIMPLE_LOG_MSG(LL_WARN,
                       "env-var %s less than min value (%lld), applying min",
                       nev->nev_name, nev->nev_min);

        nev->nev_long_value = nev->nev_min;
    }

    SIMPLE_LOG_MSG((nev->nev_long_value != nev->nev_default ?
                    LL_WARN : LL_NOTIFY),
                   "env-var %s value %lld applied from environment",
                   nev->nev_name, nev->nev_long_value);
}

static void
env_parse(const char *ev_string, struct niova_env_var *nev)
{
    if (!ev_string || !nev)
        return;

    nev->nev_present = true;
    nev->nev_rc = 0; // may be overridden in env_parse_long()

    switch (nev->nev_type)
    {
    case NIOVA_ENV_VAR_TYPE_NONE:
        SIMPLE_LOG_MSG(LL_WARN, "env-var %s detected", nev->nev_name)
        break;

    case NIOVA_ENV_VAR_TYPE_STRING:
        SIMPLE_LOG_MSG(LL_WARN, "env-var %s value %s applied from environment",
                       nev->nev_name, ev_string);
        nev->nev_string = ev_string;
        break;

    case NIOVA_ENV_VAR_TYPE_LONG:
        env_parse_long(ev_string, nev);
        break;

    default:
        nev->nev_rc = -EOPNOTSUPP;
        SIMPLE_LOG_MSG(LL_WARN,
                       "env-var %s value %s not applied (unsupported type)",
                       nev->nev_name, ev_string);

        break;
    }
}

static init_ctx_t
env_load(void)
{
    for (enum niova_env_var_num i = NIOVA_ENV_VAR_MIN;
         i < NIOVA_ENV_VAR_MAX; i++)
    {
        struct niova_env_var *nev = &niovaEnvVars[i];

        NIOVA_ASSERT(nev->nev_var_num == i);
        NIOVA_ASSERT(nev->nev_name);

        const char *ev_string = getenv(nev->nev_name);

        if (ev_string)
        {
            env_parse(ev_string, nev);

            if (nev->nev_cb)
                nev->nev_cb(nev);
        }
    }
}

const struct niova_env_var *
env_get(enum niova_env_var_num ev)
{
    NIOVA_ASSERT(niovaEnvVarsSubsysInit);
    NIOVA_ASSERT(niovaEnvVars[ev].nev_var_num == ev);

    return &niovaEnvVars[ev];
}

init_ctx_t
env_init(void)
{
    env_load();
    niovaEnvVarsSubsysInit = true;
}

destroy_ctx_t
env_destroy(void)
{
    return;
}
