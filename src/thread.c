/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE 1
#include <pthread.h>
#undef _GNU_SOURCE
#include <signal.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "log.h"
#include "thread.h"

REGISTRY_ENTRY_FILE_GENERATE;

__thread char thrName[MAX_THREAD_NAME + 1];
__thread const struct thread_ctl *thrCtl;

static void
thr_ctl_basic_sighandler(int signum)
{
    SIMPLE_LOG_MSG(LL_NOTIFY, "caught signal=%d %p", signum, thrCtl);

    if (!thrCtl)
        return;

    ((struct thread_ctl *)thrCtl)->tc_caught_stop_signal = 1;
}

static void
thread_ctl_install_sighandlers(void)
{
    static bool handlers_installed = false;
    if (handlers_installed)
        return;

    handlers_installed = true;

    struct sigaction oldact;
    FATAL_IF_strerror((sigaction(SIGALRM, NULL, &oldact)), "sigaction: ");

    struct sigaction newact = oldact;
    newact.sa_handler = &thr_ctl_basic_sighandler;
    newact.sa_flags = 0;
    sigemptyset(&newact.sa_mask);

    FATAL_IF_strerror((sigaction(SIGALRM, &newact, NULL)), "sigaction: ");
}

static thread_exec_ctx_t
thread_ctl_monitor_via_watchdog_internal(struct thread_ctl *tc)
{
    NIOVA_ASSERT(!tc->tc_watchdog);

    int rc = watchdog_add_thread(&tc->tc_watchdog_handle);

    if (!rc)
        tc->tc_watchdog = 1;

    DBG_THREAD_CTL((rc ? LL_ERROR : LL_DEBUG), tc, "%s",
                   rc ? strerror(-rc) : "");
}

/**
 * thread_ctl_monitor_via_watchdog - optional public interface for users who
 *    wish to add a thread to the watchdog at some point after calling
 *    thread_create().  However, the preferred method for watchdog usage is
 *    by calling thread_create_watched().
 *
 */
thread_exec_ctx_t
thread_ctl_monitor_via_watchdog(struct thread_ctl *tc)
{
    thread_ctl_monitor_via_watchdog_internal(tc);
}

thread_exec_ctx_bool_t
thread_ctl_should_continue(const struct thread_ctl *tc)
{
    return tc->tc_halt || tc->tc_caught_stop_signal ? false : true;
}

thread_exec_ctx_bool_t
thread_ctl_should_continue_self(void)
{
    return thrCtl ? thread_ctl_should_continue(thrCtl) : true;
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
    {
        thread_ctl_pause_if_should(tc);
        should_continue = thread_ctl_should_continue(tc);
    }

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

static int
thread_create_internal(void *(*start_routine)(void *), struct thread_ctl *tc,
                       const char *name, void *arg, const pthread_attr_t *attr,
                       bool use_watchdog)
{
    memset(tc, 0, sizeof(struct thread_ctl));

    tc->tc_arg = arg;

    int rc = pthread_create(&tc->tc_thread_id, attr, start_routine, tc);

    if (!rc && name && strnlen(name, MAX_THREAD_NAME) < MAX_THREAD_NAME)
    {
        strncpy(tc->tc_thr_name, name, MAX_THREAD_NAME);
        rc = pthread_setname_np(tc->tc_thread_id, name);

        if (!rc && use_watchdog)
            thread_ctl_monitor_via_watchdog_internal(tc);
    }

    return rc;
}

int
thread_create(void *(*start_routine)(void *), struct thread_ctl *tc,
              const char *name, void *arg, const pthread_attr_t *attr)
{
    thread_ctl_install_sighandlers();

    return thread_create_internal(start_routine, tc, name, arg, attr, false);
}

int
thread_create_watched(void *(*start_routine)(void *), struct thread_ctl *tc,
                      const char *name, void *arg, const pthread_attr_t *attr)
{
    return thread_create_internal(start_routine, tc, name, arg, attr, true);
}

void
thread_ctl_remove_from_watchdog(struct thread_ctl *tc)
{
    if (tc && tc->tc_watchdog)
        watchdog_remove_thread(&tc->tc_watchdog_handle);
}

int
thread_halt_and_destroy(struct thread_ctl *tc)
{
    thread_ctl_halt(tc);

    thread_ctl_remove_from_watchdog(tc);

    void *retval;
    int my_errno = 0;

    int kill_rc = pthread_kill(tc->tc_thread_id, SIGALRM);
    int rc = pthread_join(tc->tc_thread_id, &retval);

    if (rc)
        my_errno = errno;

    const enum log_level log_level =
        (kill_rc || rc || (long int *)retval) ? LL_WARN : LL_NOTIFY;

    DBG_THREAD_CTL(log_level, tc,
                   "pthread_join(): rc=%d:%d errno=%s, thr_retval=%p",
                   kill_rc, rc, strerror(my_errno), (long int *)retval);

    return rc;
}

void
thread_abort(void)
{
    abort();
}
