/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdlib.h>
#include <uuid/uuid.h>

#include <rocksdb/c.h>

#include "registry.h"
#include "log.h"
#include "raft.h"
#include "raft_net.h"
#include "pumice_db.h"
#include "ref_tree_proto.h"
#include "alloc.h"
#include "pumice_db.h"

#define PMDB_COLUMN_FAMILY_NAME "pumiceDB_private"

static const struct PmdbAPI *pmdbApi;
static rocksdb_column_family_handle_t *pmdbRocksdbCFH = NULL;

struct pmdb_rpc_msg
{
    uuid_t   pmrbrm_uuid; // must match rcrm_sender_id
    uint64_t pmdbrm_write_op_seqno; // op seqno stored in rocksdb + 1
    uint8_t  pmdbrm__pad[3];
    uint8_t  pmdbrm_assumed_op_type; // read or write
    uint32_t pmdbrm_data_size; // size of application payload
    char     pmdbrm_data[];
};

static rocksdb_t *
pmdb_get_rocksdb_instance(void)
{
    rocksdb_t *db = raft_server_get_rocksdb_instance(raft_net_get_instance());
    NIOVA_ASSERT(db);

    return db;
}

static rocksdb_column_family_handle_t *
pmdb_get_rocksdb_column_family_handle(void)
{
    if (!pmdbRocksdbCFH)
    {
        rocksdb_options_t *opts = rocksdb_options_create();
        if (!wopts)
            return NULL;

        char *err = NULL;
        rocksdb_options_set_create_if_missing(opts, 1);

        pmdbRocksdbCFH =
            rocksdb_create_column_family(pmdb_get_rocksdb_instance(), opts,
                                         PMDB_COLUMN_FAMILY_NAME, &err);

        rocksdb_options_destroy(opts);

        if (err)
        {
            pmdbRocksdbCFH = NULL;
            SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_create_column_family(): %s",
                           err);
        }
    }

    return pmdbRocksdbCFH;
}

static void
pmdb_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct pmdb_rpc_msg) <=
                        PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP);

    // enum PmdbOpType must fit into 'uint8_t pmdbrm_assumed_op_type'
    COMPILE_TIME_ASSERT(pmdb_op_any < (1 << sizeof(uint8_t)));
}

#define PMDB_ARG_CHECK(op, rncr)                                        \
    NIOVA_ASSERT(                                                       \
        (rncr) && (rncr)->rncr_type == op &&                            \
        (rncr)->rncr_request && (rncr)->rncr_reply &&                   \
        (rncr)->rncr_reply_data_max_size >= sizeof(struct pmdb_rpc_msg))

static int
pmdb_sm_handler_client_write_op(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_WRITE, rncr);

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;

    rocksdb_column_family_handle_t *pmdb_cfh =
        pmdb_get_rocksdb_column_family_handle();

    NIOVA_ASSERT(pmdb_cfh);
}

static int
pmdb_sm_handler_client_read_op(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    struct pmdb_rpc_msg *pmdb_reply = reply->rcrm_data;

    NIOVA_ASSERT(pmdb_req->pmdbrm_data_size <= PMDB_MAX_APP_RPC_PAYLOAD_SIZE);

    const size_t max_reply_size =
        rncr->rncr_reply_data_max_size - PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP;

    const ssize rrc =
        pmdbApi->pmdb_read(pmdb_req->pmrbrm_uuid, pmdb_req->pmdbrm_data,
                           pmdb_req->pmdbrm_data_size, pmdb_reply->pmdbrm_data,
                           max_reply_size);

    enum log_level log_level = LL_DEBUG;

    if (rrc < 0)
    {
        pmdb_reply->pmdbrm_data_size = 0;
        reply->rcrm_app_error = reply->rcrm_sys_error = (int16_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, rncr->rncr_remote_addr,
                            "pmdbApi::read(): %s", strerror(rrc));
    }
    else if (rrc > (ssize_t)max_reply_size)
    {
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;
        reply->rcrm_app_error = -E2BIG;

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, rncr->rncr_remote_addr,
                            "pmdbApi::read(): reply too large (%zd)", rrc);
    }
    else
    {
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_DEBUG, req, rncr->rncr_remote_addr,
                            "pmdbApi::read(): reply-size=%zd", rrc);
    }

    return 0;
}

static int
pmdb_sm_handler_client_op(struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE &&
                 rncr->rncr_request && rncr->rncr_reply);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    const struct raft_client_rpc_msg *req = rncr->rncr_request;

    const struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;
    const enum PmdbOpType op = pmdb_req->pmdbrm_assumed_op_type;

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, req, rncr->rncr_remote_addr, "op=%u", op);

    switch (op)
    {
    case pmdb_op_noop:
        return 0; // noop should be harmless

    case pmdb_op_read:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_READ;
        return pmdb_sm_handler_client_read_op(rncr);

    case pmdb_op_write:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_WRITE;
        return pmdb_sm_handler_client_write_op(rncr);

    default:
        break;
    }

    return -EOPNOTSUPP;
}

static int
pmdb_sm_handler_pmdb_req_check(const struct pmdb_rpc_msg *pmdb_req)
{
    if (pmdb_req->pmdbrm_data_size > PMDB_MAX_APP_RPC_PAYLOAD_SIZE)
        return -EINVAL;

    // Check the uuid for correctness
    if (uuid_is_null(pmdb_req->pmrbrm_uuid) ||
        uuid_compare(pmdb_req->pmrbrm_uuid, req->rcrm_sender_id))
        return -EBADMSG;

    return 0;
}

static int
pmdb_sm_handler(struct raft_net_client_request *rncr)
{
    if (!rncr || !rncr->rncr_request)
        return -EINVAL;

    else if (rncr->rncr_request->rcrm_data_size < sizeof(struct pmdb_rpc_msg))
        return -EBAMSG;

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;
    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    if (rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE)
    {
        if (rncr->rncr_reply_data_max_size < sizeof(struct pmdb_rpc_msg))
            return -ENOSPC;

        reply->rcrm_app_error = pmdb_sm_handler_pmdb_req_check(pmdb_req);
        if (reply->rcrm_app_error)
        {
            // There's a problem with the application RPC request
            DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, rncr->rncr_remote_addr,
                                "pmdb_sm_handler_pmdb_req_check(): %s",
                                strerror(reply->rcrm_app_error));
            return 0;
        }

        return pmdb_sm_handler_client_op(rncr);
    }

}

int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api)
{
    if (!raft_uuid_str || !raft_instance_uuid_str || !pmdb_api ||
        !pmdb_api->pmdb_apply || !pmdb_api->pmdb_read)
        return -EINVAL;

    pmdbApi = pmdb_api;

    return raft_net_server_instance_run(raft_uuid_str, raft_instance_uuid_str,
                                        pmdb_sm_handler,
                                        RAFT_INSTANCE_STORE_ROCKSDB);
}


//XXX todo
/* int */
/* PmdbClose(void) */
/* { */
//pmdb_get_rocksdb_column_family_handle()
/*     raft_net_get_instance(); */
/* } */
