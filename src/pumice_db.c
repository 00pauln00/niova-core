/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdio.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <netinet/in.h>
#include <netinet/udp.h>

#include "registry.h"
#include "log.h"
#include "raft_net.h"
#include "pumice_db.h"
#include "ref_tree_proto.h"
#include "alloc.h"
#include "pumice_db.h"

const struct PmdbAPI *pmdbApi;

static int
pmdb_sm_handler(struct raft_net_client_request *rncr)
{
    if (!rncr || !rncr->rncr_request)
        return -EINVAL;

// Check for the minimum space requirements here.
//    else if (rncr->rncr_reply_data_max_size <
//             sizeof(struct raft_test_data_block))
//        return -ENOSPC;

    // May be either read or write request, call the api to find out
    if (rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE)
    {
        const struct raft_client_rpc_msg *req = rncr->rncr_request;
        struct raft_client_rpc_msg *reply = rncr->rncr_reply;

        const enum PmdbOpType op =
            pmdbApi->pmdb_interpret(req->rcrm_sender_id, req->rcrm_data,
                                    req->rcrm_data_size);

        // The are 4 valid return values:  noop, read, write, and none.

        switch (op)
        {
        case pmdb_op_noop:
            return 0; // noop should be harmless

        case pmdb_op_none:
            return -EINVAL; // Xxx perhaps the app would to send its own rc?

        case pmdb_op_read: // Call into the api
            return pmdbApi->pmdb_read(req->rcrm_sender_id, reply->rcrm_data,
                                      reply->rcrm_data_size);

        case pmdb_op_write: /* Api layer already determined this write proceeds
                             * however, this layer must still perform its own
                             * checks */
        }
    }
}

int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api)
{
    if (!raft_uuid_str || !raft_instance_uuid_str || !pmdb_api ||
        !pmdb_api->pmdb_apply ||
        !pmdb_api->pmdb_read ||
        !pmdb_api->pmdb_interpret)
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
/*     raft_net_get_instance(); */
/* } */
