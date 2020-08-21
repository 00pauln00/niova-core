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

#include "common.h"
#include "queue.h"
#include "watchdog.h"

#define MAX_THREAD_NAME 31UL

#define THR_PAUSE_DEFAULT_USECS 1000

extern __thread char thrName[MAX_THREAD_NAME + 1];
extern __thread const struct thread_ctl *thrCtl;

struct thread_ctl
{
    char                      tc_thr_name[MAX_THREAD_NAME + 1];
    uint32_t                  tc_run:1,
                              tc_halt:1,
                              tc_user_pause_toggle:1,
                              tc_watchdog:1,
                              tc_is_utility_thread:1,
                              tc_is_watchdog_thread:1,
                              tc_caught_stop_signal:1,
                              tc_has_reached_ctl_loop:1;
    int                       tc_ret; // thread return code
    pthread_t                 tc_thread_id;
    useconds_t                tc_user_pause_usecs;
    useconds_t                tc_pause_usecs;
    void                     *tc_arg;
    struct watchdog_handle    tc_watchdog_handle;
};

#define DBG_THREAD_CTL(log_level, tc, fmt, ...)                         \
    log_msg(log_level, "tc@%p %s:%lx icnt=%lx %c%c%c%c %p "fmt,          \
            (tc), (tc)->tc_thr_name, (tc)->tc_thread_id,                \
            watchdog_get_exec_cnt(&(tc)->tc_watchdog_handle),           \
            (tc)->tc_run                     ? 'r' : '-',               \
            (tc)->tc_halt                    ? 'h' : '-',               \
            (tc)->tc_watchdog                ? 'w' : '-',               \
            (tc)->tc_has_reached_ctl_loop    ? 'l' : '-',               \
            (tc)->tc_arg, ##__VA_ARGS__)

#define THREAD_LOOP_WITH_CTL(tc)                                        \
    for (thread_ctl_set_self(tc); thread_ctl_loop_test(tc);             \
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

static inline int
thread_ctl_thread_is_halting(const struct thread_ctl *tc)
{
    return tc ? tc->tc_halt : -EINVAL;
}

static inline int
thread_ctl_thread_is_running(const struct thread_ctl *tc)
{
    return tc ? tc->tc_run : -EINVAL;
}

static inline int
thread_ctl_thread_is_watched(const struct thread_ctl *tc)
{
    return tc ? tc->tc_watchdog : -EINVAL;
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
    tc->tc_has_reached_ctl_loop = 1;
    thrCtl = tc;
}

static inline bool
thread_ctl_thread_has_reached_ctl_loop(const struct thread_ctl *tc)
{
    return tc->tc_has_reached_ctl_loop ? true : false;
}

thread_exec_ctx_bool_t
thread_ctl_should_continue_self(void);

static inline bool
thread_ctl_is_utility_thread(void)
{
    return (thrCtl && thrCtl->tc_is_utility_thread) ? true : false;
}

static inline bool
thread_ctl_is_watchdog_thread(void)
{
    return (thrCtl && thrCtl->tc_is_watchdog_thread) ? true : false;
}

void
thread_creator_wait_until_ctl_loop_reached(const struct thread_ctl *tc);

long int
thread_join(struct thread_ctl *tc);

#endif
