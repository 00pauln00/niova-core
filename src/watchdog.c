/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#define _GNU_SOURCE 1
#include <pthread.h>
#undef _GNU_SOURCE

#include "common.h"
#include "watchdog.h"
#include "ctor.h"
#include "queue.h"
#include "log.h"
#include "thread.h"
#include "registry.h"
#include "env.h"

REGISTRY_ENTRY_FILE_GENERATE;

CIRCLEQ_HEAD(wt_thread_list, watchdog_handle);

struct watchdog_instance
{
    pthread_mutex_t       wdi_mutex;
    pthread_cond_t        wdi_cond;
    struct wt_thread_list wdi_thread_list;
    unsigned int          wdi_init:1,
                          wdi_is_default:1,
                          wdi_halt:1,
                          wdi_timeout_sec:10,
                          wdi_num_stalls_allowed:10;
    struct thread_ctl     wdi_thread_ctl;
};

static struct watchdog_instance defaultWdi = {
    .wdi_mutex              = PTHREAD_MUTEX_INITIALIZER,
    .wdi_cond               = PTHREAD_COND_INITIALIZER,
    .wdi_timeout_sec        = WATCHDOG_DEFAULT_FREQUENCY,
    .wdi_num_stalls_allowed = WATCHDOG_DEFAULT_STALL_CNT,
    .wdi_is_default         = 1,
};

#define WDI_LOCK(wdi)                                           \
    FATAL_IF_strerror((pthread_mutex_lock(&(wdi)->wdi_mutex)), \
                      "pthread_mutex_lock(): ")

#define WDI_UNLOCK(wdi)                                            \
    FATAL_IF_strerror((pthread_mutex_unlock(&(wdi)->wdi_mutex)), \
                      "pthread_mutex_unlock(): ")

#define WDI_SIGNAL_LOCKED(wdi)                                 \
{                                                              \
    FATAL_IF_strerror((pthread_cond_signal(&(wdi)->wdi_cond)), \
                      "pthread_mutex_unlock(): ");             \
    SIMPLE_LOG_MSG(LL_DEBUG, "here");                           \
}

static const struct thread_ctl *
watchdog_handle_2_thread_ctl(const struct watchdog_handle *wdh)
{
    return (const struct thread_ctl *)((const char *)wdh -
                                       offsetof(struct thread_ctl,
                                                tc_watchdog_handle));
}

int
watchdog_add_thread(struct watchdog_handle *wdh)
{
    if (!wdh)
        return -EINVAL;
    else if (!CIRCLEQ_ENTRY_DETACHED(&wdh->wdh_lentry))
        return -EALREADY;
    else if (!defaultWdi.wdi_init)
        return -EAGAIN;

    struct watchdog_instance *wdi = &defaultWdi;

    WDI_LOCK(wdi);

    wdh->wdh_exec_cnt_tracker = 0;
    wdh->wdh_current_stalls = 0;

    CIRCLEQ_INSERT_TAIL(&wdi->wdi_thread_list, wdh, wdh_lentry);

    WDI_SIGNAL_LOCKED(wdi);

    WDI_UNLOCK(wdi);

    DBG_THREAD_CTL(LL_NOTIFY, watchdog_handle_2_thread_ctl(wdh), "");

    return 0;
}

int
watchdog_remove_thread(struct watchdog_handle *wdh)
{
    if (!wdh)
        return -EINVAL;
    else if (CIRCLEQ_ENTRY_DETACHED(&wdh->wdh_lentry))
        return -EALREADY;
    else if (!defaultWdi.wdi_init)
        return -EAGAIN;

    struct watchdog_instance *wdi = &defaultWdi;
    struct watchdog_handle *my_wdh;
    bool removed = false;

    WDI_LOCK(wdi);

    CIRCLEQ_FOREACH(my_wdh, &wdi->wdi_thread_list, wdh_lentry)
    {
        if (my_wdh == wdh)
        {
            CIRCLEQ_REMOVE(&wdi->wdi_thread_list, wdh, wdh_lentry);
            removed = true;
            break;
        }
    }

    WDI_SIGNAL_LOCKED(wdi);

    WDI_UNLOCK(wdi);

    DBG_THREAD_CTL(LL_NOTIFY, watchdog_handle_2_thread_ctl(wdh), "rc=%d",
                   removed ? 0 : -ENOENT);

    return removed ? 0 : -ENOENT;
}

static int
watchdog_subsystem_init_env_var_apply(struct watchdog_instance *wdi);

static watchdog_exec_ctx_int_t
watchdog_instance_init(struct watchdog_instance *wdi)
{
    WDI_LOCK(wdi);

    if (wdi->wdi_init || wdi->wdi_halt)
    {
        WDI_UNLOCK(wdi);
        return -EALREADY;
    }

    watchdog_subsystem_init_env_var_apply(wdi);

    // .. just support one watchdog for now.
    NIOVA_ASSERT(wdi->wdi_is_default);

    CIRCLEQ_INIT(&wdi->wdi_thread_list);
    wdi->wdi_init = 1;

    WDI_UNLOCK(wdi);

    return 0;
}

static watchdog_exec_ctx_t
watchdog_track_threads(struct watchdog_instance *wdi)
{
    struct watchdog_handle *wdh = NULL;

    WDI_LOCK(wdi);
    CIRCLEQ_FOREACH(wdh, &wdi->wdi_thread_list, wdh_lentry)
    {
        const struct thread_ctl *thr_ctl = watchdog_handle_2_thread_ctl(wdh);

        /* Only monitor threads which are running
         */
        if (thread_ctl_thread_is_watched(thr_ctl) &&
            thread_ctl_thread_is_running(thr_ctl) &&
            !thread_ctl_thread_is_halting(thr_ctl))
        {
            if (wdh->wdh_thread_exec_cnt == wdh->wdh_exec_cnt_tracker)
                wdh->wdh_current_stalls++;
            else
                wdh->wdh_exec_cnt_tracker = wdh->wdh_thread_exec_cnt;
        }

        DBG_THREAD_CTL(LL_DEBUG, thr_ctl, "cur-stalls=%lx",
                       wdh->wdh_current_stalls);
    }
    WDI_UNLOCK(wdi);
}

static watchdog_exec_ctx_t *
watchdog_svc_thread(void *arg)
{
    SIMPLE_LOG_MSG(LL_DEBUG, "hello");

    struct thread_ctl *tc = arg;
    NIOVA_ASSERT(tc);

    tc->tc_is_watchdog_thread = 1;

    thread_ctl_set_self(tc);

    struct watchdog_instance *wdi = tc->tc_arg;
    NIOVA_ASSERT(wdi);

    struct timespec ts;

    THREAD_LOOP_WITH_CTL(tc)
    {
        niova_realtime_clock(&ts);
        ts.tv_sec += wdi->wdi_timeout_sec;

        WDI_LOCK(wdi);

        if (!wdi->wdi_halt)
        {
            int rc =
                pthread_cond_timedwait(&wdi->wdi_cond, &wdi->wdi_mutex, &ts);

            FATAL_IF_strerror((rc && rc != ETIMEDOUT),
                              "pthread_cond_timedwait() (rc=%d): ", rc);
        }

        WDI_UNLOCK(wdi);

        watchdog_track_threads(wdi);

        if (wdi->wdi_halt)
            break;
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye");

    return (void *)0;
}

static int
watchdog_subsystem_init_env_var_apply(struct watchdog_instance *wdi)
{
    const struct niova_env_var *ev;

    ev = env_get(NIOVA_ENV_VAR_watchdog_disable);
    if (ev && ev->nev_present)
        return -EAGAIN;

    ev = env_get(NIOVA_ENV_VAR_watchdog_frequency);
    if (ev && ev->nev_present)
        wdi->wdi_timeout_sec = ev->nev_long_value;

    ev = env_get(NIOVA_ENV_VAR_watchdog_stall_cnt);
    if (ev && ev->nev_present)
        wdi->wdi_num_stalls_allowed = ev->nev_long_value;

    return 0;
}

init_ctx_t
watchdog_subsystem_init(void)
{
    struct watchdog_instance *wdi = &defaultWdi;

    int rc = watchdog_instance_init(wdi);
    if (rc)
        return;

    rc = thread_create(watchdog_svc_thread, &wdi->wdi_thread_ctl,
                       "watchdog", wdi, NULL);

    FATAL_IF_strerror(rc, "pthread_create(): ");

    thread_ctl_run(&wdi->wdi_thread_ctl);
}

destroy_ctx_t
watchdog_subsystem_destroy(void)
{
    struct watchdog_instance *wdi = &defaultWdi;

    if (!wdi->wdi_init)
        return;

    WDI_LOCK(wdi);

    wdi->wdi_halt = 1;
    WDI_SIGNAL_LOCKED(wdi);

    WDI_UNLOCK(wdi);

    int rc = thread_halt_and_destroy(&wdi->wdi_thread_ctl);

    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye, svc thread: %s", strerror(-rc));
}
