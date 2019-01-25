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

thread_exec_ctx_bool_t
thread_ctl_should_continue(const struct thread_ctl *tc)
{
    return tc->tc_halt ? false : true;
}

thread_exec_ctx_bool_t
thread_ctl_should_pause(struct thread_ctl *tc)
{
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
        return true;
    }

    return false;
}

thread_exec_ctx_t
thread_ctl_pause(struct thread_ctl *tc)
{
    int rc = usleep(tc->tc_pause_usecs);
    if (rc < 0)
    {
        rc = errno;
        NIOVA_ASSERT(rc == EINTR);
    }

    tc->tc_pause_usecs = 0;
}

thread_exec_ctx_t
thread_ctl_pause_if_should(struct thread_ctl *tc)
{
    while (thread_ctl_should_pause(tc))
        thread_ctl_pause(tc);
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
    int rc = pthread_join(tc->tc_thread_id, &retval);

    if (rc)
        rc = -errno;

    const enum log_level log_level =
        (rc || (long int *)retval) ? LL_WARN : LL_NOTIFY;

    DBG_THREAD_CTL(log_level, tc, "pthread_join(): %s, thr_retval=%p",
                   strerror(rc), (long int *)retval);

    return rc;
}

void
thread_abort(void)
{
    abort();
}
