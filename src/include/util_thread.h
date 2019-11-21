/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _UTIL_THREAD_H_
#define _UTIL_THREAD_H_ 1

#include "common.h"
#include "ctor.h"
#include "epoll_mgr.h"

typedef void util_thread_ctx_t;
typedef void util_thread_ctx_reg_t;
typedef int  util_thread_ctx_reg_int_t;

typedef void util_thread_ctx_ctli_t;
typedef char util_thread_ctx_ctli_char_t;

int
util_thread_install_event_src(int fd, int events,
                              void (*ut_cb)(const struct epoll_handle *),
                              void *arg);

init_ctx_t
util_thread_subsystem_init(void)
    __attribute__ ((constructor (UTIL_THREAD_SUBSYS_CTOR_PRIORITY)));

destroy_ctx_t
util_thread_subsystem_destroy(void)
    __attribute__ ((destructor (UTIL_THREAD_SUBSYS_CTOR_PRIORITY)));

#endif
