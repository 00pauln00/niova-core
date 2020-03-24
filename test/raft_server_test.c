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
#include "raft_test.h"
#include "ref_tree_proto.h"
#include "alloc.h"

#define OPTS "u:r:h"

const char *raft_uuid_str;
const char *my_uuid_str;

REGISTRY_ENTRY_FILE_GENERATE;

struct rst_sm_node_app
{
    struct raft_test_values smna_committed;
    struct raft_test_values smna_pending;
    uint64_t                smna_pending_msg_id;
    struct sockaddr_in      smna_pending_client_addr;
    int64_t                 smna_pending_entry_term;
};

struct rst_sm_node
{
    uuid_t                      smn_uuid;
    REF_TREE_ENTRY(rst_sm_node) smn_rtentry;
    struct rst_sm_node_app      smn_app;
};

static inline int
rst_sm_node_cmp(const struct rst_sm_node *a, const struct rst_sm_node *b)
{
    return uuid_compare(a->smn_uuid, b->smn_uuid);
}

REF_TREE_HEAD(rst_sm_node_tree, rst_sm_node);
REF_TREE_GENERATE(rst_sm_node_tree, rst_sm_node, smn_rtentry, rst_sm_node_cmp);

static struct rst_sm_node_tree smNodeTree;

static struct rst_sm_node *
rst_sm_node_lookup(const uuid_t lookup_uuid, const bool add)
{
    struct rst_sm_node lookup_sm;

    uuid_copy(lookup_sm.smn_uuid, lookup_uuid);

    return RT_GET(rst_sm_node_tree, &smNodeTree, &lookup_sm, add, NULL);
}

static void
rst_sm_check_or_apply_values(struct rst_sm_node *sm,
                             const struct raft_test_data_block *rtdb,
                             bool check, int *ret)
{
    NIOVA_ASSERT(sm && rtdb);
    NIOVA_ASSERT(rtdb->rtdb_num_values &&
                 rtdb->rtdb_num_values <= RAFT_TEST_VALUES_MAX);
    if (check)
        NIOVA_ASSERT(ret);

    struct raft_test_values *sm_rtv = &sm->smn_app.smna_committed;

    for (uint16_t i = 0; i < rtdb->rtdb_num_values; i++)
    {
        const struct raft_test_values *new_rtv = &rtdb->rtdb_values[i];

        DBG_RAFT_TEST_DATA_BLOCK(
            LL_NOTIFY, rtdb,
            "exseqno=%lu nseqno:val=%lu:%lu check=%d i=%hu",
            sm_rtv->rtv_seqno, new_rtv->rtv_seqno, new_rtv->rtv_request_value,
            check, i);

        if (check)
        {
            if ((sm_rtv->rtv_seqno + 1 + i) != new_rtv->rtv_seqno)
            {
                *ret = -EILSEQ;
                return;
            }
        }
        else
        {
            sm_rtv->rtv_seqno++;
            sm_rtv->rtv_reply_xor_all_values ^= new_rtv->rtv_request_value;

            NIOVA_ASSERT(sm_rtv->rtv_seqno == new_rtv->rtv_seqno);
        }
    }
}

static int
rst_sm_check_values(struct rst_sm_node *sm,
                    const struct raft_test_data_block *rtdb)
{
    int rc = 0;

    rst_sm_check_or_apply_values(sm, rtdb, true, &rc);

    return rc;
}

static void
rst_sm_apply_values(struct rst_sm_node *sm,
                    const struct raft_test_data_block *rtdb)
{
    rst_sm_check_or_apply_values(sm, rtdb, false, NULL);
}

static int
rst_sm_handler_commit(struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_commit_data &&
                 rncr->rncr_reply_data_max_size <= RAFT_TEST_VALUES_MAX);

    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)rncr->rncr_commit_data;

    NIOVA_ASSERT(rtdb->rtdb_op == RAFT_TEST_DATA_OP_WRITE);
    NIOVA_ASSERT(rtdb->rtdb_num_values > 0);

    // Add the item if it doesn't exist, this may be startup or a new leader.
    struct rst_sm_node *sm = rst_sm_node_lookup(rtdb->rtdb_client_uuid, true);
    struct rst_sm_node_app *sma = &sm->smn_app;

    // Malloc failure here is unrecoverable.
    FATAL_IF((!sm), "rst_sm_node_lookup(): %s", strerror(ENOMEM));

    int rc = 0;

    if (sma->smna_pending_entry_term == rncr->rncr_current_term)
    {
        const uint16_t num_rtv = rtdb->rtdb_num_values;
        const struct raft_test_values *rtv = &rtdb->rtdb_values[num_rtv - 1];

        FATAL_IF((memcmp(&sm->smn_app.smna_pending, rtv,
                         sizeof(struct raft_test_values))),
                 "smna_pending %ld:%ld does not match entry data %ld:%ld",
                 sma->smna_pending.rtv_seqno,
                 sma->smna_pending.rtv_reply_xor_all_values,
                 rtv->rtv_seqno, rtv->rtv_reply_xor_all_values);

        // Entry was written by this leader.
        rncr->rncr_remote_addr = sma->smna_pending_client_addr;
        rncr->rncr_msg_id = sma->smna_pending_msg_id;
    }
    else
    {
        // Cannot reply to the client since the info is from a previous term.
        rc = -ESTALE;
    }

    rst_sm_apply_values(sm, rtdb);

    return rc;
}

/**
 * rst_sm_handler_write -
 *    NOTE:  returning with an error will cause an immediate reply to the
 *    client.
 */
static int
rst_sm_handler_write(struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_request && rncr->rncr_is_leader &&
                 rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_WRITE);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    // Map the raft_test_data_block from the request data
    const struct raft_client_rpc_msg *request = rncr->rncr_request;
    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)request->rcrm_data;

    // Map the last rtv in the array after verify range
    const uint16_t num_rtv = rtdb->rtdb_num_values;

    if (!num_rtv || num_rtv > RAFT_TEST_VALUES_MAX)
    {
        reply->rcrm_app_error = -EINVAL;
        return -EINVAL;
    }
    const struct raft_test_values *last_rtv = &rtdb->rtdb_values[num_rtv - 1];

    // State machine app info
    struct rst_sm_node *rst_sm =
        rst_sm_node_lookup(rtdb->rtdb_client_uuid, true);

    struct rst_sm_node_app *sma = &rst_sm->smn_app;

    if (!rst_sm) // Malloc failure
    {
        reply->rcrm_sys_error = -ENOMEM;
        return -ENOMEM;
    }

    if (last_rtv->rtv_seqno <= sma->smna_committed.rtv_seqno)
    {
        /* Client sees 'ok', return an error to the caller so that this
         * request does not land in the log.
         */
        return -EALREADY;
    }

    /* The pending info this in this item was written by self in the current
     * term.  These checks are only valid if this info is on / for the current
     * leader and this node is the current leader (which it should be since
     * this is a write request).  Note that it's possible that several leader
     * elections have occurred where this node was elected more than once and
     * contains stale info from its prior leader session.
     */
    if (sma->smna_pending_entry_term == rncr->rncr_current_term)
    {
        NIOVA_ASSERT((int64_t)sma->smna_pending.rtv_seqno >=
                     (int64_t)sma->smna_committed.rtv_seqno);

        if (last_rtv->rtv_seqno <= sma->smna_pending.rtv_seqno)
        {
            // Is this a retried pending request?
            reply->rcrm_app_error = -EINPROGRESS;
            return -EINPROGRESS;
        }
        else if (sma->smna_pending.rtv_seqno != ID_ANY_64bit)
        {
            /* Check if a request is already pending.  Clients may only have a
             * single write operation in-flight at any time.  This check must
             * be done after verifying that this is not a delayed retry (to
             * which the server should reply OK).
             */
            reply->rcrm_app_error = -EBUSY;
            return -EBUSY;
        }
    }

    /* Ensure the sequence of this request is correct.  This could happen
     * if the client was buggy and issued an out-of-order request.
     */
    int rc = rst_sm_check_values(rst_sm, rtdb);
    if (rc)
    {
         reply->rcrm_app_error = rc;
    }
    else
    {
        // Store some context for the reply (which will happen later)
        sma->smna_pending_msg_id = request->rcrm_msg_id;
        sma->smna_pending_entry_term = rncr->rncr_current_term;
        sma->smna_pending_client_addr = rncr->rncr_remote_addr;
        sma->smna_pending = *last_rtv;
    }

    DBG_RAFT_TEST_DATA_BLOCK(LL_NOTIFY, rtdb, "msg-id=%lu term=%lu rc=%s",
                             request->rcrm_msg_id, rncr->rncr_current_term,
                             strerror(-rc));

    return rc;
}

static int
rst_sm_handler_read(struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_request &&
                 rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_READ);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    // Errors returned here are service related, not application related
    if (!reply)
        return -EINVAL;

    else if (rncr->rncr_reply_data_max_size < sizeof(struct raft_test_values))
        return -ENOSPC;

    const struct raft_client_rpc_msg *rrm = rncr->rncr_request;
    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)rrm->rcrm_data;

    struct rst_sm_node *rst_sm =
        rst_sm_node_lookup(rtdb->rtdb_client_uuid, false);

    if (!rst_sm)
    {
        reply->rcrm_app_error = -ENOENT;
    }
    else
    {
        reply->rcrm_data_size = sizeof(struct raft_test_values);

        memcpy(reply->rcrm_data, &rst_sm->smn_app.smna_committed,
               sizeof(struct raft_test_values));
    }

    return 0;
}

/**
 * rst_sm_handler_verify_request_and_set_type - preparation function called
 *    in advance of the read or write handlers.  This function
 *    inspects the app-specific msg contents for the operation type and
 *    validity.
 */
static int
rst_sm_handler_verify_request_and_set_type(
    struct raft_net_client_request *rncr)
{
    if (!rncr || !rncr->rncr_request || !rncr->rncr_reply ||
        rncr->rncr_type != RAFT_NET_CLIENT_REQ_TYPE_NONE)
        return -EINVAL;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    const struct raft_client_rpc_msg *request = rncr->rncr_request;

    if (request->rcrm_data_size < sizeof(struct raft_test_data_block))
    {
        /* This request does not have the proper application payload size, but
         * is otherwise a validated msg, send a reply notifying the client of
         * the problem.
         */
        reply->rcrm_sys_error = reply->rcrm_app_error = -EBADMSG;
        return -EBADMSG;
    }

    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)request->rcrm_data;

    DBG_RAFT_TEST_DATA_BLOCK(LL_DEBUG, rtdb, "");

    /* Check the UUID inside the payload - only the payload will be written
     * into raft so it's vital to ensure the UUID matches that of the sender.
     * Note that payload specific checks cannot be done in the server due to
     * scoping contstraints - the server (raft_server.c) may only validate the
     * contents of struct raft_client_rpc_msg.
     */
    if (uuid_compare(rtdb->rtdb_client_uuid, request->rcrm_sender_id))
    {
        reply->rcrm_sys_error = reply->rcrm_app_error = -ENODEV;
        return -ENODEV;
    }
    else if (rtdb->rtdb_op != RAFT_TEST_DATA_OP_READ &&
             rtdb->rtdb_op != RAFT_TEST_DATA_OP_WRITE)
    {
        reply->rcrm_sys_error = reply->rcrm_app_error = -ENOTSUP;
        return -ENOTSUP;
    }
    else if (rtdb->rtdb_num_values > RAFT_TEST_VALUES_MAX)
    {
        reply->rcrm_sys_error = reply->rcrm_app_error = -EOVERFLOW;
        return -EOVERFLOW;
    }

    // rtdb_num_values should be 0 for reads.
    const size_t expected_size =
        (sizeof(struct raft_test_data_block) +
         rtdb->rtdb_num_values * sizeof(struct raft_test_values));

    if (expected_size != request->rcrm_data_size ||
        (rtdb->rtdb_op == RAFT_TEST_DATA_OP_WRITE && !rtdb->rtdb_num_values))
    {
        reply->rcrm_sys_error = reply->rcrm_app_error = -EMSGSIZE;
        return -EMSGSIZE;
    }

    // Set the type for the upper layer.
    rncr->rncr_type = rtdb->rtdb_op == RAFT_TEST_DATA_OP_READ ?
        RAFT_NET_CLIENT_REQ_TYPE_READ : RAFT_NET_CLIENT_REQ_TYPE_WRITE;

    return 0;
}

/**
 * raft_server_test_rst_sm_handler - general state machine handler for the
 *    raft test app.
 */
static int
raft_server_test_rst_sm_handler(struct raft_net_client_request *rncr)
{
    if (!rncr || !rncr->rncr_request)
        return -EINVAL;

    /* Requests have 3 logical types:  read, write, and commit.  Commit
     * requests are effectively "completed writes".  However, reads and writes
     * coming from the client must be interpreted to determine what type of
     * request they may be.  If our caller specifies
     * RAFT_NET_CLIENT_REQ_TYPE_NONE, this means that this is either a read or
     * write and it's the reponsibility of
     * rst_sm_handler_verify_request_and_set_type() to determine this for us.
     */
    if (rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE)
    {
        /* rst_sm_handler_verify_request_and_set_type() will set the error
         * in the RPC reply as needed.
         */
        int rc = rst_sm_handler_verify_request_and_set_type(rncr);

        if (rc)
        {
            DBG_RAFT_CLIENT_RPC(
                LL_NOTIFY, rncr->rncr_request, &rncr->rncr_remote_addr,
                "rst_sm_handler_verify_request_and_set_type(): %s",
                strerror(-rc));

            return rc;
        }
    }

    switch (rncr->rncr_type)
    {
    case RAFT_NET_CLIENT_REQ_TYPE_READ:
        return rst_sm_handler_read(rncr);

    case RAFT_NET_CLIENT_REQ_TYPE_WRITE:
        return rst_sm_handler_write(rncr);

    case RAFT_NET_CLIENT_REQ_TYPE_COMMIT:
        return rst_sm_handler_commit(rncr);

    default:
        break;
    }

    return -EINVAL;
}

static void
rst_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r UUID -n UUID\n", argv[0]);

    exit(error);
}

static void
rst_getopt(int argc, char **argv)
{
    if (!argc || !argv)
        return;

    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'r':
            raft_uuid_str = optarg;
            break;
        case 'u':
            my_uuid_str = optarg;
            break;
        case 'h':
            rst_print_help(0, argv);
            break;
        default:
            rst_print_help(EINVAL, argv);
            break;
        }
    }

    if (!raft_uuid_str || !my_uuid_str)
        rst_print_help(EINVAL, argv);
}

static struct rst_sm_node *
rst_sm_node_construct(const struct rst_sm_node *in)
{
    if (!in)
        return NULL;

    struct rst_sm_node *sm =
        niova_calloc((size_t)1, sizeof(struct rst_sm_node));

    if (!sm)
        return NULL;

    uuid_copy(sm->smn_uuid, in->smn_uuid);

    sm->smn_app.smna_committed.rtv_seqno = ID_ANY_64bit;
    sm->smn_app.smna_pending.rtv_seqno = ID_ANY_64bit;
    sm->smn_app.smna_pending_msg_id = ID_ANY_64bit;
    sm->smn_app.smna_pending_entry_term = ID_ANY_64bit;

    return sm;
}

static int
rst_sm_node_destruct(struct rst_sm_node *destroy)
{
    if (!destroy)
        return -EINVAL;

    niova_free(destroy);

    return 0;
}

int
main(int argc, char **argv)
{
    rst_getopt(argc, argv);

    REF_TREE_INIT_ALT_REF(&smNodeTree, rst_sm_node_construct,
                          rst_sm_node_destruct, 2);

    return raft_net_server_instance_run(raft_uuid_str, my_uuid_str,
                                        raft_server_test_rst_sm_handler);
}
