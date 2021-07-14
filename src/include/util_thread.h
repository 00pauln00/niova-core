/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _UTIL_THREAD_H_
#define _UTIL_THREAD_H_ 1

#include "init.h"
#include "ctor.h"
#include "epoll_mgr.h"

typedef void util_thread_ctx_t;
typedef void util_thread_ctx_reg_t;
typedef int  util_thread_ctx_reg_int_t;

typedef void util_thread_ctx_ctli_t;
typedef int  util_thread_ctx_ctli_int_t;
typedef char util_thread_ctx_ctli_char_t;

bool
util_thread_ctx(void);

int
util_thread_install_event_src(int fd, int events,
                              epoll_mgr_cb_t ut_cb,
                              void *arg, struct epoll_handle **ret_eph);

int
util_thread_remove_event_src(struct epoll_handle *eph);

#endif
