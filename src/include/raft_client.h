/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_RAFT_CLIENT_H
#define NIOVA_RAFT_CLIENT_H 1

#include <sys/uio.h>

#include "common.h"
#include "env.h"
#include "raft_net.h"
#include "util.h"

#define RAFT_CLIENT_REQUEST_TIMEOUT_MAX_SECS 0xffffffffU
#define RAFT_CLIENT_REQUEST_TIMEOUT_SECS 60

enum raft_client_request_opts
{
    RCRT_READ                     = 0,
    RCRT_WRITE                    = (1 << 0),
    RCRT_NON_BLOCKING             = (1 << 1),
    RCRT_USE_PROVIDED_READ_BUFFER = (1 << 2),
};

typedef struct raft_client_leader_info
{
    uuid_t  rcli_leader_uuid;
    size_t  rcli_leader_alive_cnt;
    bool    rcli_leader_viable;
} raft_client_leader_info_t;

#define RAFT_CLIENT_REQUEST_HANDLE_MAX_IOVS 8

typedef void * raft_client_thread_t;
typedef int  raft_client_app_ctx_int_t;   // raft client app thread
typedef void raft_client_app_ctx_t;
typedef void * raft_client_instance_t;

typedef int (*raft_client_data_2_obj_id_t)(const char *, const size_t,
                                           struct raft_net_client_user_id *);

typedef void (*raft_client_user_cb_t)(void *, ssize_t, void *);
int
raft_client_init(const char *raft_uuid_str, const char *raft_client_uuid_str,
                 raft_client_data_2_obj_id_t obj_id_cb,
                 raft_client_instance_t *client_instance,
                 enum raft_instance_store_type server_store_type);

int
raft_client_destroy(raft_client_instance_t client_instance);

int
raft_client_request_cancel(raft_client_instance_t rci,
                           const struct raft_net_client_user_id *rncui,
                           const char *reply_buf);

void
raft_client_set_default_request_timeout(unsigned int timeout);

static inline void
raft_client_set_default_request_timeout_env_cb(const struct niova_env_var *nev)
{
    if (nev)
        return;

    raft_client_set_default_request_timeout((unsigned int)nev->nev_long_value);
}

unsigned int
raft_client_get_default_request_timeout(void);

int
raft_client_request_submit(raft_client_instance_t rci,
                           const struct raft_net_client_user_id *rncui,
                           const struct iovec *src_iovs, size_t nsrc_iovs,
                           struct iovec *dest_iovs, size_t ndest_iovs,
                           bool allocate_get_buffer_for_user,
                           const struct timespec timeout,
                           const enum raft_client_request_opts rcrt,
                           raft_client_user_cb_t user_cb, void *user_arg,
                           const raft_net_request_tag_t tag);

int
raft_client_get_leader_info(raft_client_instance_t client_instance,
                            raft_client_leader_info_t *leader_info);
#endif
