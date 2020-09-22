/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_RAFT_CLIENT_H
#define NIOVA_RAFT_CLIENT_H 1

#include <sys/uio.h>

#include "common.h"
#include "raft_net.h"
#include "util.h"

enum raft_client_request_type
{
    RCRT_READ,     // blocking
    RCRT_READ_NB,  // non-blocking
    RCRT_WRITE,
    RCRT_WRITE_NB
};

#define RAFT_CLIENT_REQUEST_HANDLE_MAX_IOVS 8

typedef void * raft_client_thread_t;
typedef int  raft_client_app_ctx_int_t;   // raft client app thread
typedef void raft_client_app_ctx_t;
typedef void * raft_client_instance_t;

typedef int (*raft_client_data_2_obj_id_t)(const char *, const size_t,
                                           struct raft_net_client_user_id *);

typedef void (*raft_client_user_cb_t)(void *, ssize_t);
int
raft_client_init(const char *raft_uuid_str, const char *raft_client_uuid_str,
                 raft_client_data_2_obj_id_t obj_id_cb,
                 raft_client_instance_t *client_instance);

int
raft_client_destroy(raft_client_instance_t client_instance);

int
raft_client_request_cancel(raft_client_instance_t rci,
                           const struct raft_net_client_user_id *rncui,
                           const char *reply_buf);

int
raft_client_request_submit(raft_client_instance_t rci,
                           const struct raft_net_client_user_id *rncui,
                           const struct iovec *src_iovs, size_t nsrc_iovs,
                           struct iovec *dest_iovs, size_t ndest_iovs,
                           const struct timespec timeout,
                           const enum raft_client_request_type rcrt,
                           raft_client_user_cb_t user_cb, void *user_arg,
                           const raft_net_request_tag_t tag);
#endif
