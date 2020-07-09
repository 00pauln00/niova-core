/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_RAFT_CLIENT_H
#define NIOVA_RAFT_CLIENT_H 1

#include "common.h"
#include "raft_net.h"
#include "util.h"

// This is the same as the number of total pending requests
#define RAFT_CLIENT_MAX_SUB_APP_INSTANCES       \
    (RAFT_CLIENT_MAX_INSTANCES * 4096)

typedef void * raft_client_thread_t;
typedef int    raft_client_app_ctx_int_t; // raft client app thread
typedef void   raft_client_app_ctx_t;
typedef void * raft_client_instance_t;

int
raft_client_init(const char *raft_uuid_str, const char *raft_client_uuid_str,
                 raft_client_instance_t *client_instance);

int
raft_client_destroy(raft_client_instance_t client_instance);

int
raft_client_request_cancel(raft_client_instance_t rci,
                           const raft_net_client_user_id *rncui,
                           const char *reply_buf);

int
raft_client_request_submit(raft_client_instance_t rci,
                           const raft_net_client_user_id *rncui,
                           const char *request,
                           const size_t request_size,
                           char *reply, const size_t reply_size,
                           const struct timespec timeout,
                           const bool block,
                           (void)(*cb)(const raft_net_client_user_id *,
                                       void *, char *, size_t, int),
                           void *arg);
#endif
