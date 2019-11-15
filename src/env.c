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
};

static void
env_parse_long(const char *ev_string, struct niova_env_var *ev)
{
    if (!ev_string || !ev)
        return;

    ev->nev_long_value = strtoll(ev_string, NULL, 10);

    if (ev->nev_long_value == LLONG_MIN || ev->nev_long_value == LLONG_MAX)
    {
        ev->nev_rc = -errno;
        ev->nev_long_value = -1;

        SIMPLE_LOG_MSG(LL_WARN, "env-var %s has invalid value (%s): %s",
                       ev->nev_name, ev_string, strerror(-ev->nev_rc));

        return;
    }

    if (ev->nev_long_value > ev->nev_max)
    {
        ev->nev_rc = -ERANGE;

        SIMPLE_LOG_MSG(LL_WARN,
                       "env-var %s exceeds the max value (%lld), applying max",
                       ev->nev_name, ev->nev_max);

        ev->nev_long_value = ev->nev_max;
    }
    else if (ev->nev_long_value < ev->nev_min)
    {
        ev->nev_rc = -ERANGE;

        SIMPLE_LOG_MSG(LL_WARN,
                       "env-var %s less than min value (%lld), applying min",
                       ev->nev_name, ev->nev_min);

        ev->nev_long_value = ev->nev_min;
    }

    SIMPLE_LOG_MSG(LL_WARN,
                   "env-var %s value %lld applied from environment",
                   ev->nev_name, ev->nev_long_value);
}

static void
env_parse(const char *ev_string, struct niova_env_var *ev)
{
    ev->nev_present = true;

    switch (ev->nev_type)
    {
    case NIOVA_ENV_VAR_TYPE_NONE:
        SIMPLE_LOG_MSG(LL_WARN, "env-var %s detected", ev->nev_name);
        break;

    case NIOVA_ENV_VAR_TYPE_STRING:
        SIMPLE_LOG_MSG(LL_WARN, "env-var %s value %s applied from environment",
                       ev->nev_name, ev_string);
        ev->nev_string = ev_string;
        break;

    case NIOVA_ENV_VAR_TYPE_LONG:
        env_parse_long(ev_string, ev);
        break;

    default:
        SIMPLE_LOG_MSG(LL_WARN,
                       "env-var %s value %s not applied (unsupported type)",
                       ev->nev_name, ev_string);

        break;
    }
}

static init_ctx_t
env_load(void)
{
    for (enum niova_env_var_num i = NIOVA_ENV_VAR_MIN;
         i < NIOVA_ENV_VAR_MAX; i++)
    {
        NIOVA_ASSERT(niovaEnvVars[i].nev_var_num == i);
        NIOVA_ASSERT(niovaEnvVars[i].nev_name);

        const char *ev_string = getenv(niovaEnvVars[i].nev_name);
        if (ev_string)
            env_parse(ev_string, (struct niova_env_var *)&niovaEnvVars[i]);
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
