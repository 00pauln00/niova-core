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
#define PMDB_MAX_APP_RPC_PAYLOAD_SIZE_UDP \
    RAFT_NET_MAX_RPC_SIZE - RAFT_NET_MAX_RPC_SIZE

#define PMDB_MAX_APP_RPC_PAYLOAD_SIZE PMDB_MAX_APP_RPC_PAYLOAD_SIZE_UDP

typedef raft_client_instance_t          pmdb_t;
typedef struct raft_net_client_user_key pmdb_obj_id_t;

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

typedef struct pmdb_rpc_msg
{
    struct raft_net_client_user_id pmdbrm_user_id;
    int64_t                        pmdbrm_write_seqno; // request::next,
                                                   //  reply::committed
    uint8_t                        pmdbrm_op;
    uint8_t                        pmdbrm_write_pending;  // reply context only
    uint8_t                        pmdbrm__pad[2];
    int32_t                        pmdbrm_err; // reply ctx error
    uint32_t                       pmdbrm_data_size; // size of data payload
    char                           pmdbrm_data[];
} PmdbRpcMsg_t;

static inline size_t
pmdb_net_calc_rpc_msg_size(const struct pmdb_rpc_msg *pmdb_rpc_msg)
{
    size_t size = 0;

    if (pmdb_rpc_msg)
        size = (sizeof(struct pmdb_rpc_msg) + pmdb_rpc_msg->pmdbrm_data_size);

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

#endif
