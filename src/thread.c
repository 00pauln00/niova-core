/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE 1
#include <pthread.h>
#undef _GNU_SOURCE

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "log.h"
#include "thread.h"

REGISTRY_ENTRY_FILE_GENERATE;

__thread char thrName[MAX_THREAD_NAME + 1];

thread_exec_ctx_t
thread_ctl_monitor_via_watchdog(struct thread_ctl *tc)
{
    NIOVA_ASSERT(!tc->tc_watchdog);

    int rc = watchdog_add_thread(&tc->tc_watchdog_handle);

    if (!rc)
        tc->tc_watchdog = 1;

    DBG_THREAD_CTL((rc ? LL_ERROR : LL_DEBUG), tc, "%s",
                   rc ? strerror(-rc) : "");
}

thread_exec_ctx_bool_t
thread_ctl_should_continue(const struct thread_ctl *tc)
{
    return tc->tc_halt ? false : true;
}

static thread_exec_ctx_t
thread_ctl_inc_iteration_cnt(struct thread_ctl *tc)
{
    if (tc)
        watchdog_inc_exec_cnt(&tc->tc_watchdog_handle);
}

thread_exec_ctx_bool_t
thread_ctl_should_pause(struct thread_ctl *tc)
{
    /* Bump the iteration counter here so that continual pauses don't
     * trigger the watchdog.
     */
    thread_ctl_inc_iteration_cnt(tc);

    if (!thread_ctl_should_continue(tc))
    {
        /* Thread is halting, no need to wait.
         */
        return false;
    }
    else if (!tc->tc_run)
    {
        /* Thread control asked this thread to stop running for now.
         */
        tc->tc_pause_usecs = THR_PAUSE_DEFAULT_USECS;

        return true;
    }
    else if (tc->tc_user_pause_usecs)
    {
        /* The thread itself has asked to pause.
         */
        tc->tc_pause_usecs = tc->tc_user_pause_usecs;
        tc->tc_user_pause_toggle = !tc->tc_user_pause_toggle;

        return tc->tc_user_pause_toggle ? false : true;
    }

    return false;
}

thread_exec_ctx_t
thread_ctl_pause(struct thread_ctl *tc)
{
    FATAL_IF_strerror((usleep(tc->tc_pause_usecs) && errno != EINTR),
                      "usleep() failure");

    tc->tc_pause_usecs = 0;
}

thread_exec_ctx_t
thread_ctl_pause_if_should(struct thread_ctl *tc)
{
    while (thread_ctl_should_pause(tc))
        thread_ctl_pause(tc);
}

thread_exec_ctx_bool_t
thread_ctl_loop_test(struct thread_ctl *tc)
{
    bool should_continue = thread_ctl_should_continue(tc);

    if (should_continue)
        thread_ctl_pause_if_should(tc);

    return should_continue;
}

thread_exec_ctx_t
thread_ctl_set_user_pause_usec(struct thread_ctl *tc, uint32_t usecs)
{
    tc->tc_user_pause_usecs = usecs;
}

void
thread_ctl_run(struct thread_ctl *tc)
{
    tc->tc_run = 1;

    DBG_THREAD_CTL(LL_NOTIFY, tc, "");
}

void
thread_ctl_halt(struct thread_ctl *tc)
{
    tc->tc_halt = 1;

    DBG_THREAD_CTL(LL_NOTIFY, tc, "");
}

thread_id_t
thread_id_get(void)
{
    return pthread_self();
}

const char *
thread_name_get(void)
{
    pthread_getname_np(pthread_self(), thrName, MAX_THREAD_NAME);
    return (const char *)thrName;
}

int
thread_create(void *(*start_routine)(void *), struct thread_ctl *tc,
              const char *name, void *arg, const pthread_attr_t *attr)
{
    memset(tc, 0, sizeof(struct thread_ctl));

    tc->tc_arg = arg;

    int rc = pthread_create(&tc->tc_thread_id, attr, start_routine, tc);

    if (!rc && name && strnlen(name, MAX_THREAD_NAME) < MAX_THREAD_NAME)
    {
        strncpy(tc->tc_thr_name, name, MAX_THREAD_NAME);
        rc = pthread_setname_np(tc->tc_thread_id, name);
    }

    return rc;
}

int
thread_halt_and_destroy(struct thread_ctl *tc)
{
    thread_ctl_halt(tc);

    void *retval;
    int my_errno = 0;
    int rc = pthread_join(tc->tc_thread_id, &retval);

    if (rc)
        my_errno = errno;

    const enum log_level log_level =
        (rc || (long int *)retval) ? LL_WARN : LL_NOTIFY;

    DBG_THREAD_CTL(log_level, tc,
                   "pthread_join(): rc=%d errno=%s, thr_retval=%p",
                   rc, strerror(my_errno), (long int *)retval);

    return rc;
}

void
thread_abort(void)
{
    abort();
}
