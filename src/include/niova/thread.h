/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef _THREAD_H_
#define _THREAD_H_ 1

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include <pthread.h>
#include <unistd.h>

#include "atomic.h"
#include "niova/common.h"
#include "niova/queue.h"
#include "niova/watchdog.h"

#define MAX_THREAD_NAME 31UL

#define THR_PAUSE_DEFAULT_USECS 1000

extern __thread char thrName[MAX_THREAD_NAME + 1];
extern __thread const struct thread_ctl *thrCtl;

enum tc_flags
{
    TC_FLAG_RUN              = (1 << 0),
    TC_FLAG_HALT             = (1 << 1),
    TC_FLAG_WATCHDOG         = (1 << 2),
    TC_FLAG_IS_WATCHDOG_THR  = (1 << 3),
    TC_FLAG_IS_UTILITY_THR   = (1 << 4),
    TC_FLAG_EXITED           = (1 << 5),
    TC_FLAG_REACHED_CTL_LOOP = (1 << 6),
};

struct thread_ctl
{
    char                   tc_thr_name[MAX_THREAD_NAME + 1];
    enum tc_flags          tc_flags;
    int                    tc_ret;    // thread return code
    pthread_t              tc_thread_id;
    useconds_t             tc_pause_usecs;
    void                  *tc_arg;
    size_t                 tc_sig_cnt;
    struct watchdog_handle tc_watchdog_handle;
    pthread_mutex_t       *tc_waiting_mutex;
    pthread_cond_t        *tc_waiting_cond;
};

static inline enum tc_flags
thread_ctl_has_flag(const struct thread_ctl *tc, enum tc_flags flag)
{
    if (tc == NULL)
        return 0;

    return niova_atomic_read(&tc->tc_flags) & flag;
}

static inline int
thread_ctl_set_flag(struct thread_ctl *tc, enum tc_flags flag)
{
    bool cas_ok = false;

    do {
        enum tc_flags current = tc->tc_flags;

        if (current & flag)
            return -EALREADY;

        cas_ok = niova_atomic_cas(&tc->tc_flags, current, (current | flag));

    } while (!cas_ok);

    return 0;
}

static inline int
thread_ctl_unset_flag(struct thread_ctl *tc, enum tc_flags flag)
{
    bool cas_ok = false;

    do {
        enum tc_flags current = tc->tc_flags;

        if (!(current & flag))
            return -EALREADY;

        cas_ok = niova_atomic_cas(&tc->tc_flags, current, (current & ~flag));

    } while (!cas_ok);

    return 0;
}

#define DBG_THREAD_CTL(log_level, tc, fmt, ...)                         \
    log_msg(log_level, "tc@%p %s:%lx icnt=%lu scnt=%lu %c%c%c%c %p "fmt, \
            (tc), (tc)->tc_thr_name, (tc)->tc_thread_id,                \
            watchdog_get_exec_cnt(&(tc)->tc_watchdog_handle),           \
            (tc)->tc_sig_cnt,                                           \
            thread_ctl_has_flag((tc), TC_FLAG_RUN)              ? 'r' : '-', \
            thread_ctl_has_flag((tc), TC_FLAG_HALT)             ? 'h' : '-', \
            thread_ctl_has_flag((tc), TC_FLAG_WATCHDOG)         ? 'w' : '-', \
            thread_ctl_has_flag((tc), TC_FLAG_REACHED_CTL_LOOP) ? 'l' : '-', \
            (tc)->tc_arg, ##__VA_ARGS__)

#define THREAD_LOOP_WITH_CTL(tc)                            \
    for (thread_ctl_set_self(tc); thread_ctl_loop_test(tc); \
         thread_ctl_pause_if_should(tc))

static inline void *
thread_ctl_get_arg(struct thread_ctl *tc)
{
    return tc ? tc->tc_arg : NULL;
}

thread_exec_ctx_bool_t
thread_ctl_loop_test(struct thread_ctl *);

thread_exec_ctx_bool_t
thread_ctl_should_pause(struct thread_ctl *);

thread_exec_ctx_bool_t
thread_ctl_should_continue(const struct thread_ctl *);

thread_exec_ctx_t
thread_ctl_pause_if_should(struct thread_ctl *);

thread_exec_ctx_t
thread_ctl_pause(struct thread_ctl *);

thread_exec_ctx_t
thread_ctl_set_user_pause_usec(struct thread_ctl *, uint32_t);

void
thread_ctl_run(struct thread_ctl *);

void
thread_ctl_halt(struct thread_ctl *);

static inline bool
thread_ctl_thread_is_halting(const struct thread_ctl *tc)
{
    return (tc && thread_ctl_has_flag(tc, TC_FLAG_HALT)) ?
        true : false;
}

static inline bool
thread_ctl_thread_is_running(const struct thread_ctl *tc)
{
    return (tc && thread_ctl_has_flag(tc, TC_FLAG_RUN)) ?
        true : false;
}

static inline bool
thread_ctl_thread_is_watched(const struct thread_ctl *tc)
{
    return (tc && thread_ctl_has_flag(tc, TC_FLAG_WATCHDOG)) ? true : false;
}

static inline void
thread_ctl_apply_cond_and_mutex(struct thread_ctl *tc, pthread_cond_t *cond,
                                pthread_mutex_t *mutex)
{
    if (tc && cond && mutex)
    {
        tc->tc_waiting_cond = cond;
        tc->tc_waiting_mutex = mutex;
    }
}

thread_id_t
thread_id_get(void);

const char *
thread_name_get(void);

int
thread_create(void *(*)(void *), struct thread_ctl *, const char *, void *,
              const pthread_attr_t *);

int
thread_create_watched(void *(*)(void *), struct thread_ctl *, const char *,
                      void *, const pthread_attr_t *);

int
thread_halt_and_destroy(struct thread_ctl *);

void
thread_abort(void);

thread_exec_ctx_t
thread_ctl_monitor_via_watchdog(struct thread_ctl *tc);

void
thread_ctl_remove_from_watchdog(struct thread_ctl *tc);

static inline void
thread_ctl_set_self(struct thread_ctl *tc)
{
    int rc = thread_ctl_set_flag(tc, TC_FLAG_REACHED_CTL_LOOP);
    if (rc == 0)
        thrCtl = tc;

    else if (tc != thrCtl) // 'self' may be set only once
        thread_abort();
}

static inline bool
thread_ctl_thread_has_reached_ctl_loop(const struct thread_ctl *tc)
{
    return (thread_ctl_has_flag(tc, (TC_FLAG_REACHED_CTL_LOOP |
                                     TC_FLAG_EXITED))) ? true : false;
}

thread_exec_ctx_bool_t
thread_ctl_should_continue_self(void);

static inline bool
thread_ctl_is_utility_thread(void)
{
    return (thrCtl && thread_ctl_has_flag(thrCtl, TC_FLAG_IS_UTILITY_THR)) ?
            true : false;
}

static inline bool
thread_ctl_is_watchdog_thread(void)
{
    return (thrCtl && thread_ctl_has_flag(thrCtl, TC_FLAG_IS_WATCHDOG_THR)) ?
            true : false;
}

void
thread_creator_wait_until_ctl_loop_reached(const struct thread_ctl *tc);

long int
thread_join(struct thread_ctl *tc);

long int
thread_join_nb(struct thread_ctl *tc);

int
thread_issue_sig_alarm_to_thread(pthread_t tid);

static inline bool
thread_has_exited(const struct thread_ctl *tc)
{
    return (tc && thread_ctl_has_flag(tc, TC_FLAG_EXITED)) ? true : false;
}
#endif
