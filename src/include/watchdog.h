/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _WATCHDOG_H_
#define _WATCHDOG_H_ 1

#include "ctor.h"
#include "common.h"
#include "queue.h"

#define	WATCHDOG_DEFAULT_FREQUENCY 60
#define WATCHDOG_MAX_FREQUENCY     1000
#define WATCHDOG_MIN_FREQUENCY     10

#define WATCHDOG_DEFAULT_STALL_CNT 4
#define WATCHDOG_MAX_STALL_CNT     32
#define WATCHDOG_MIN_STALL_CNT     2

typedef thread_exec_ctx_u64_t watchdog_exec_ctx_u64_t;
typedef void watchdog_exec_ctx_t;
typedef int watchdog_exec_ctx_int_t;

struct watchdog_handle
{
    watchdog_exec_ctx_u64_t        CACHE_ALIGN_MEMBER(wdh_exec_cnt_tracker);
    watchdog_exec_ctx_u64_t        wdh_current_stalls;
    thread_exec_ctx_u64_t          CACHE_ALIGN_MEMBER(wdh_thread_exec_cnt);
    CIRCLEQ_ENTRY(watchdog_handle) wdh_lentry;
};

int
watchdog_add_thread(struct watchdog_handle *wdh);

int
watchdog_remove_thread(struct watchdog_handle *wdh);

static inline thread_exec_ctx_t
watchdog_inc_exec_cnt(struct watchdog_handle *wdh)
{
    if (wdh)
        wdh->wdh_thread_exec_cnt++;
}

static inline watchdog_exec_ctx_u64_t
watchdog_get_exec_cnt(const struct watchdog_handle *wdh)
{
    return wdh ? wdh->wdh_thread_exec_cnt : 0;
}

init_ctx_t
watchdog_subsystem_init(void)
    __attribute__ ((constructor (WATCHDOG_SUBSYS_CTOR_PRIORITY)));

destroy_ctx_t
watchdog_subsystem_destroy(void)
    __attribute__ ((destructor (WATCHDOG_SUBSYS_CTOR_PRIORITY)));

#endif
