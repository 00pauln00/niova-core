/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_PUMICE_DB_NET_H_
#define __NIOVA_PUMICE_DB_NET_H_ 1

#include <uuid/uuid.h>

#include "common.h"
#include "raft_client.h"
#include "raft_net.h"

#define PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP 1024
#define PMDB_MAX_APP_RPC_PAYLOAD_SIZE_UDP                                \
    (raft_net_max_rpc_size(RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP) - \
     PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP)

#define PMDB_MAX_APP_RPC_PAYLOAD_SIZE PMDB_MAX_APP_RPC_PAYLOAD_SIZE_UDP
#define PMDB_MAX_REQUEST_IOVS RAFT_CLIENT_REQUEST_HANDLE_MAX_IOVS

//extern const struct raft_net_client_user_key pmdbReadAnyRncuiKey;
/* pmdbReadAnyRncuiKey may be used by an application to bypass the pmdb object
 * existence check on the server.  On the client, requests using this key may
 * be done concurrently.  Note that writes attempting to this key will fail
 * with -EPERM.
 */
static const struct raft_net_client_user_key pmdbReadAnyRncuiKey = {
    .v0.rncui_v0_uint64 = { -1ULL, -1ULL, -1ULL, -1ULL, -1ULL, -1ULL},
};

typedef raft_client_instance_t          pmdb_t;
typedef struct raft_net_client_user_key pmdb_obj_id_t;
static niova_atomic32_t pmdb_read_any_cnt = 0;

enum PmdbOpType
{
    pmdb_op_noop   = 0,
    pmdb_op_lookup = 1,
    pmdb_op_read   = 2,
    pmdb_op_write  = 3,
    pmdb_op_apply  = 4,
    pmdb_op_reply  = 5,
    pmdb_op_none   = 6,
    pmdb_op_any    = 7,
};

static inline const char *pmdp_op_2_string(enum PmdbOpType op)
{
    switch (op)
    {
    case pmdb_op_noop:   return "noop";
    case pmdb_op_lookup: return "lookup";
    case pmdb_op_read:   return "read";
    case pmdb_op_write:  return "write";
    case pmdb_op_apply:  return "apply";
    case pmdb_op_reply:  return "reply";
    default: break;
    }
    return "unknown";
}

#define PMDB_MSG_MAGIC 0x1a2b3c4

struct pmdb_msg
{
    uint32_t                       pmdbrm_magic;
    uint32_t                       pmdbrm_crc;
    struct raft_net_client_user_id pmdbrm_user_id;
    int64_t                        pmdbrm_write_seqno; // request::next,
    //  reply::committed
    uint8_t                        pmdbrm_op;
    uint8_t                        pmdbrm_write_pending;  // reply context only
    uint8_t                        pmdbrm__pad[2];
    int32_t                        pmdbrm_err; // reply ctx error
    uint32_t                       pmdbrm_data_size; // size of data payload
    uint32_t                       pmdbrm_pad2;
    char                           WORD_ALIGN_MEMBER(pmdbrm_data[]);
};

typedef struct pmdb_obj_stat
{
    pmdb_obj_id_t obj_id;
    int64_t       sequence_num;
    int64_t       reply_size;
    int           status;
    uint8_t       write_op_pending : 1;
    void         *reply_buffer;
} pmdb_obj_stat_t;

typedef void (*pmdb_user_cb_t)(void *, ssize_t);

/**
 * pmdb_request_options - user parameters for 'advanced' behaviors such as
 *   user provided buffer buffers, asynchronous requests, custom timeouts,
 *   and obtaining system object information.
 * @pro_non_blocking:  async request
 * @pro_stat:  user requests pmdb sequence number information.
 * @pro_non_blocking_cb:  user async cb handler
 * @pro_arg:  user async cb arg
 * @pro_get_buffer:  user provided sink buffer
 * @pro_get_buffer_size:  sink buffer size
 * @pro_timeout:  operation timeout
 */
typedef struct pmdb_request_options
{
    uint8_t          pro_non_blocking:1,
                     pro_get_response:1;
    pmdb_obj_stat_t *pro_stat;
    pmdb_user_cb_t   pro_non_blocking_cb;
    void            *pro_arg;
    void            *pro_get_buffer;
    size_t           pro_get_buffer_size;
    struct timespec  pro_timeout;
} pmdb_request_opts_t;

static inline size_t
pmdb_net_calc_rpc_msg_size(const struct pmdb_msg *pmdb_msg)
{
    size_t size = 0;

    if (pmdb_msg)
        size = (sizeof(struct pmdb_msg) + pmdb_msg->pmdbrm_data_size);

    return size;
}

static inline raft_client_instance_t
pmdb_2_rci(pmdb_t pmdb)
{
    return pmdb ? (raft_client_instance_t)pmdb : NULL;
}

static inline const struct raft_net_client_user_id *
pmdb_obj_id_2_rncui(const pmdb_obj_id_t *obj_id,
                    struct raft_net_client_user_id *out)
{
    if (!obj_id || !out)
        return NULL;

    out->rncui_key = *(struct raft_net_client_user_key *)obj_id;
    out->rncui_version = 0;

    return out;
}

static inline unsigned int
pmdb_get_default_request_timeout(void)
{
    return raft_client_get_default_request_timeout();
}

static inline void
pmdb_request_options_init(pmdb_request_opts_t *pmdb_req, int use_user_buffer,
                  int non_blocking,
                  int get_response,
                  pmdb_obj_stat_t *obj_stat, pmdb_user_cb_t user_cb,
                  void *user_arg,
                  void *get_buffer,
                  size_t get_buffer_size,
                  int timeout_sec)
{
    pmdb_req->pro_non_blocking = non_blocking;
    pmdb_req->pro_get_response = get_response;
    pmdb_req->pro_stat = obj_stat;
    pmdb_req->pro_non_blocking_cb = user_cb;
    pmdb_req->pro_arg = user_arg;
    pmdb_req->pro_get_buffer = get_buffer;
    pmdb_req->pro_get_buffer_size = get_buffer_size;
    pmdb_req->pro_timeout.tv_sec = timeout_sec;
    pmdb_req->pro_timeout.tv_nsec = 0;
}

static inline int
pmdb_rncui_set_read_any(struct raft_net_client_user_id *out)
{
    switch (out->rncui_version)
    {
    case 0:
        memcpy(&out->rncui_key.v0, &pmdbReadAnyRncuiKey.v0,
               sizeof(struct raft_net_client_user_key_v0));
        // Assign application read_counter in the last part of rncui
        out->rncui_key.v0.rncui_v0_uint64[5] =
                    niova_atomic_inc(&pmdb_read_any_cnt);
        return 0;
    default:
        break;
    }

    return -EOPNOTSUPP;
}


static inline bool
pmdb_rncui_is_read_any(const struct raft_net_client_user_id *in)
{
    switch (in->rncui_version)
    {
    case 0:
        return memcmp(&in->rncui_key.v0, &pmdbReadAnyRncuiKey.v0,
                      sizeof(struct raft_net_client_user_key_v0) - sizeof(uint64_t)) ?
            false : true;
    default:
        break;
    }

    return false;
}

static inline void
pmdb_msg_init(struct pmdb_msg *msg, uint32_t data_size, uint8_t op,
              int64_t sequence_num)
{
    msg->pmdbrm_magic = PMDB_MSG_MAGIC;
    msg->pmdbrm_op = op;
    msg->pmdbrm_data_size = data_size;
    if (op == pmdb_op_write)
        msg->pmdbrm_write_seqno = sequence_num;
}

#endif
