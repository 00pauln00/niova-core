/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <regex.h>
#include <unistd.h>
#include <uuid/uuid.h>

#include "common.h"
#include "log.h"
#include "alloc.h"
#include "pumice_db_client.h"
#include "raft_test.h"
#include "random.h"
#include "regex_defines.h"
#include "thread.h"
#include "util_thread.h"

#define OPTS "au:r:h"

#define PMDB_TEST_CLIENT_MAX_APPS 128
#define PMDB_TEST_CLIENT_REQ_HIST_SZ 128

static struct thread_ctl pmdbtcThrCtl;
static regex_t pmdbtcCmdRegex;

static const char *raft_uuid_str;
static const char *my_uuid_str;
static bool use_async_requests = false;

static pmdb_t pmdbtcPMDB;

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

enum pmdb_lreg_values
{
    PMDB_TEST_CLIENT_LREG_SUB_APP,
    PMDB_TEST_CLIENT_LREG_REQ_HISTORY,
    PMDB_TEST_CLIENT_LREG_CMD_INPUT,
    PMDB_TEST_CLIENT_LREG__MAX,
};

enum pmdb_test_app_lreg_values
{
    PMDB_TEST_APP_LREG_RNCUI,
    PMDB_TEST_APP_LREG_STATUS,
    PMDB_TEST_APP_LREG_PMDB_SEQNO,
    PMDB_TEST_APP_LREG_WRITE_PENDING,
    PMDB_TEST_APP_LREG_LAST_REQUEST_TIME,
    PMDB_TEST_APP_LREG_LAST_REQUEST_TAG,
    PMDB_TEST_APP_LREG_APP_SYNC,
    PMDB_TEST_APP_LREG_APP_SEQNO,
    PMDB_TEST_APP_LREG_APP_VALUE,
    PMDB_TEST_APP_LREG_APP_VALIDATED,
    PMDB_TEST_APP_LREG_APP__MAX,
};

enum pmdb_test_req_lreg_values
{
    PMDB_TEST_REQ_LREG_RNCUI,
    PMDB_TEST_REQ_LREG_PMDB_OP,
    PMDB_TEST_REQ_LREG_STATUS,
    PMDB_TEST_REQ_LREG_PMDB_REQ_SEQNO,
    PMDB_TEST_REQ_LREG_PMDB_SEQNO,
    PMDB_TEST_REQ_LREG_WRITE_PENDING,
    PMDB_TEST_REQ_LREG_LAST_REQUEST_TIME,
    PMDB_TEST_REQ_LREG_LAST_REQUEST_TAG,
    PMDB_TEST_REQ_LREG_APP_SEQNO,
    PMDB_TEST_REQ_LREG_APP_VALUE,
    PMDB_TEST_REQ_LREG_APP__MAX,
};

REGISTRY_ENTRY_FILE_GENERATE;

#define PMDB_RTV_MAX 1

struct pmdbtc_app
{
    struct raft_net_client_user_id papp_rncui;
    pmdb_obj_stat_t                papp_obj_stat;
    raft_net_request_tag_t         papp_last_tag;
    struct timespec                papp_last_request;
    raft_net_request_tag_t         papp_pending_tag;
    struct random_data             papp_random_data;
    uint8_t                        papp_sync : 1;
    char                           papp_random_state_buf[RANDOM_STATE_BUF_LEN];
    struct raft_test_values        papp_rtv;
    struct raft_test_values        papp_last_rtv_request;
    struct raft_test_values        papp_last_rtv_validated;
};

struct pmdbtc_request
{
    struct pmdbtc_app             *preq_papp;
    struct raft_net_client_user_id preq_rncui;
    enum PmdbOpType                preq_op;
    size_t                         preq_op_cnt;
    int64_t                        preq_write_seqno;
    STAILQ_ENTRY(pmdbtc_request)   preq_lentry;
    raft_net_request_tag_t         preq_last_tag;
    pmdb_obj_stat_t                preq_obj_stat;
    struct timespec                preq_submitted;
    struct raft_test_data_block    preq_rtdb; // preq_rtv must follow!
    struct raft_test_values        preq_rtv[PMDB_RTV_MAX];
};

static util_thread_ctx_ctli_int_t pmdbtcNumApps;
static util_thread_ctx_ctli_int_t pmdbtcHistoryCnt;
static struct pmdbtc_app pmdbtcApps[PMDB_TEST_CLIENT_MAX_APPS];
static struct pmdbtc_request pmdbtcReqHistory[PMDB_TEST_CLIENT_REQ_HIST_SZ];

STAILQ_HEAD(pmdbtc_request_queue, pmdbtc_request);
static struct pmdbtc_request_queue preqQueue =
    STAILQ_HEAD_INITIALIZER(preqQueue);

static util_thread_ctx_reg_int_t
pmdbtc_lreg_cb(enum lreg_node_cb_ops, struct lreg_value *, void *);

LREG_ROOT_ENTRY_GENERATE_OBJECT(pumice_db_test_client,
                                LREG_USER_TYPE_RAFT_CLIENT_APP,
                                PMDB_TEST_CLIENT_LREG__MAX,
                                pmdbtc_lreg_cb, NULL);

static util_thread_ctx_reg_int_t
pmdbtc_test_apps_varray_lreg_cb(enum lreg_node_cb_ops op,
                                struct lreg_node *lrn, struct lreg_value *lv)
{
    if (!lv)
        return -EINVAL;

    lv->get.lrv_num_keys_out = PMDB_TEST_APP_LREG_APP__MAX;

    NIOVA_ASSERT(lrn->lrn_vnode_child);

    struct lreg_vnode_data *vd = &lrn->lrn_lvd;
    if (vd->lvd_index >= pmdbtcNumApps)
        return -ERANGE;

    const struct pmdbtc_app *papp = &pmdbtcApps[vd->lvd_index];

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;
        strncpy(lv->lrv_key_string, "pmdbtc-apps", LREG_VALUE_STRING_MAX);
        strncpy(LREG_VALUE_TO_OUT_STR(lv), "none", LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
        break;
    case LREG_NODE_CB_OP_READ_VAL:

        switch (lv->lrv_value_idx_in)
        {
        case PMDB_TEST_APP_LREG_RNCUI:
            lreg_value_fill_key_and_type(lv, "app-user-id",
                                         LREG_VAL_TYPE_STRING);

            raft_net_client_user_id_to_string(&papp->papp_rncui,
                                              LREG_VALUE_TO_OUT_STR(lv),
                                              LREG_VALUE_STRING_MAX);
            break;
        case PMDB_TEST_APP_LREG_STATUS:
            lreg_value_fill_string(lv, "status",
                                   strerror(ABS(papp->papp_obj_stat.status)));
            break;
        case PMDB_TEST_APP_LREG_PMDB_SEQNO:
            lreg_value_fill_signed(lv, "pmdb-seqno",
                                   papp->papp_obj_stat.sequence_num);
            break;
        case PMDB_TEST_APP_LREG_WRITE_PENDING:
            lreg_value_fill_bool(lv, "pmdb-write-pending",
                                 papp->papp_obj_stat.write_op_pending ?
                                 true : false);
            break;
        case PMDB_TEST_APP_LREG_LAST_REQUEST_TAG:
            lreg_value_fill_unsigned(lv, "last-request-tag",
                                     papp->papp_last_tag);
            break;
        case PMDB_TEST_APP_LREG_LAST_REQUEST_TIME:
            lreg_value_fill_string_time(lv, "last-request",
                                        papp->papp_last_request.tv_sec);
            break;
        case PMDB_TEST_APP_LREG_APP_SYNC:
            lreg_value_fill_bool(lv, "app-sync",
                                 papp->papp_sync ? true : false);
            break;
        case PMDB_TEST_APP_LREG_APP_SEQNO:
            lreg_value_fill_unsigned(lv, "app-seqno",
                                     papp->papp_rtv.rtv_seqno);
            break;
        case PMDB_TEST_APP_LREG_APP_VALUE:
            lreg_value_fill_unsigned(lv, "app-value",
                                     papp->papp_rtv.rtv_reply_xor_all_values);
            break;
        case PMDB_TEST_APP_LREG_APP_VALIDATED:
            lreg_value_fill_unsigned(lv, "app-validated-seqno",
                                     papp->papp_last_rtv_validated.rtv_seqno);
            break;
        default:
            break;
        }
        break;
    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
        return -ENOENT;
    }
    return 0;
}

static util_thread_ctx_reg_int_t
pmdbtc_request_history_varray_lreg_cb(enum lreg_node_cb_ops op,
                                      struct lreg_node *lrn,
                                      struct lreg_value *lv)
{
    if (!lv)
        return -EINVAL;

    lv->get.lrv_num_keys_out = PMDB_TEST_REQ_LREG_APP__MAX;

    NIOVA_ASSERT(lrn->lrn_vnode_child);

    struct lreg_vnode_data *vd = &lrn->lrn_lvd;
    if (vd->lvd_index >= pmdbtcHistoryCnt)
        return -ERANGE;

    const struct pmdbtc_request *preq = &pmdbtcReqHistory[vd->lvd_index];

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;
        strncpy(lv->lrv_key_string, "pmdbtc-completed-req",
                LREG_VALUE_STRING_MAX);
        strncpy(LREG_VALUE_TO_OUT_STR(lv), "none", LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
        break;
    case LREG_NODE_CB_OP_READ_VAL:

        switch (lv->lrv_value_idx_in)
        {
        case PMDB_TEST_REQ_LREG_RNCUI:
            lreg_value_fill_key_and_type(lv, "app-user-id",
                                         LREG_VAL_TYPE_STRING);

            raft_net_client_user_id_to_string(&preq->preq_rncui,
                                              LREG_VALUE_TO_OUT_STR(lv),
                                              LREG_VALUE_STRING_MAX);
            break;
        case PMDB_TEST_REQ_LREG_PMDB_OP:
            lreg_value_fill_string(lv, "op", pmdp_op_2_string(preq->preq_op));
            break;
        case PMDB_TEST_REQ_LREG_STATUS:
            lreg_value_fill_string(lv, "status",
                                   strerror(ABS(preq->preq_obj_stat.status)));
            break;
        case PMDB_TEST_REQ_LREG_PMDB_REQ_SEQNO:
            lreg_value_fill_signed(lv, "pmdb-req-seqno",
                                   preq->preq_write_seqno);
            break;
        case PMDB_TEST_REQ_LREG_PMDB_SEQNO:
            lreg_value_fill_signed(lv, "pmdb-seqno",
                                   preq->preq_obj_stat.sequence_num);
            break;
        case PMDB_TEST_REQ_LREG_WRITE_PENDING:
            lreg_value_fill_bool(lv, "pmdb-write-pending",
                                 preq->preq_obj_stat.write_op_pending ?
                                 true : false);
            break;
        case PMDB_TEST_REQ_LREG_LAST_REQUEST_TAG:
            lreg_value_fill_unsigned(lv, "last-request-tag",
                                     preq->preq_last_tag);
            break;
        case PMDB_TEST_REQ_LREG_LAST_REQUEST_TIME:
            lreg_value_fill_string_time(lv, "submitted-time",
                                        preq->preq_submitted.tv_sec);
            break;
        case PMDB_TEST_REQ_LREG_APP_SEQNO:
            lreg_value_fill_unsigned(lv, "app-seqno",
                                     preq->preq_rtv[0].rtv_seqno);
            break;
        case PMDB_TEST_REQ_LREG_APP_VALUE:
            lreg_value_fill_unsigned(lv, "app-value",
                                     preq->preq_rtv[0].rtv_request_value);
            break;
        default:
            break;
        }
        break;
    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
        return -ENOENT;
    }
    return 0;
}

static void
pmdbtc_app_rand_init(struct pmdbtc_app *papp)
{
    NIOVA_ASSERT(papp);

    papp->papp_rtv.rtv_seqno = 0;
    papp->papp_rtv.rtv_reply_xor_all_values = 0;

    uint32_t seed = random_create_seed_from_uuid(
        RAFT_NET_CLIENT_USER_ID_2_UUID(&papp->papp_rncui, 0, 0));

    int rc = initstate_r(seed, papp->papp_random_state_buf,
                         RANDOM_STATE_BUF_LEN, &papp->papp_random_data);

    FATAL_IF((rc), "initstate_r(): %s", strerror(-rc));
}

static void
pmdbtc_app_init(const struct raft_net_client_user_id *rncui,
                struct pmdbtc_app *papp)
{
    NIOVA_ASSERT(rncui && papp);
    memset(papp, 0, sizeof(struct pmdbtc_app));

    raft_net_client_user_id_copy(&papp->papp_rncui, rncui);
    pmdbtc_app_rand_init(papp);
}

static struct pmdbtc_app *
pmdbtc_app_lookup(const struct raft_net_client_user_id *rncui, bool add)
{
    for (size_t i = 0; i < PMDB_TEST_CLIENT_MAX_APPS; i++)
        if (!raft_net_client_user_id_cmp(rncui, &pmdbtcApps[i].papp_rncui))
            return &pmdbtcApps[i];

    if (add && (pmdbtcNumApps < PMDB_TEST_CLIENT_MAX_APPS))
    {
        const size_t idx = pmdbtcNumApps++;
        pmdbtc_app_init(rncui, &pmdbtcApps[idx]);

        return &pmdbtcApps[idx];
    }

    return NULL;
}

static void
pmdbtc_app_history_add(const struct pmdbtc_request *preq)
{
    if (preq)
    {
        const size_t idx = pmdbtcHistoryCnt++ % PMDB_TEST_CLIENT_REQ_HIST_SZ;
        pmdbtcReqHistory[idx] = *preq;
    }
}

static util_thread_ctx_ctli_int_t
pmdbtc_queue_request(const struct raft_net_client_user_id *rncui,
                     enum PmdbOpType op, size_t op_cnt,
                     const int64_t write_seqno)
{
    if (!rncui)
        return -EINVAL;

    else if (!(op == pmdb_op_lookup || op == pmdb_op_read ||
               op == pmdb_op_write))
        return -EOPNOTSUPP;

    struct pmdbtc_app *papp = pmdbtc_app_lookup(rncui, true);
    if (!papp)
        return -ENOSPC;

    struct pmdbtc_request *preq =
        niova_calloc_can_fail(1UL, sizeof(struct pmdbtc_request));

    if (!preq)
        return -ENOMEM;

    // Initialize the preq with the provided info
    preq->preq_papp = papp;
    preq->preq_op = op;
    preq->preq_op_cnt = op_cnt;
    preq->preq_write_seqno = write_seqno;
    niova_realtime_coarse_clock(&preq->preq_submitted);

    raft_net_client_user_id_copy(&preq->preq_rncui, rncui);

    // Move request to the worker thread
    NIOVA_SET_COND_AND_WAKE(
        signal, {STAILQ_INSERT_TAIL(&preqQueue, preq, preq_lentry);},
        &mutex, &cond);

    return 0;
}

static util_thread_ctx_ctli_int_t
pmdbtc_parse_and_process_input_cmd(const char *input_cmd_str)
{
    if (!input_cmd_str)
        return -EINVAL;

    int rc = regexec(&pmdbtcCmdRegex, input_cmd_str, 0, NULL, 0);

    SIMPLE_LOG_MSG(LL_DEBUG, "input=%s (regex-rc=%d)", input_cmd_str, rc);

    if (rc)
        return -EBADMSG;

    struct raft_net_client_user_id rncui = {0};

    char local_str[LREG_VALUE_STRING_MAX + 1] = {0};
    strncpy(local_str, input_cmd_str, LREG_VALUE_STRING_MAX);

    int64_t write_seqno = -1ULL;
    char *uuid_str = NULL;

    enum PmdbOpType op = pmdb_op_any;
    size_t op_cnt = 1;

    /* Cmd string formats:
     * <RNCUI_V0_REGEX_BASE>.<read>|(<write>.<seqno>)
     */
    const char *sep = ".";
    char *sp = NULL;
    size_t pos = 0;
    for (char *sub = strtok_r(local_str, sep, &sp);
         sub != NULL;
         sub = strtok_r(NULL, sep, &sp), pos++)
    {
        switch (pos)
        {
        case 0:
            rc = raft_net_client_user_id_parse(sub, &rncui, 0);
            if (rc)
                return -EBADMSG;

            uuid_str = sub;
            uuid_str[UUID_STR_LEN - 1] = '\0';

            break;
        case 1:
            if (!strncmp("read", sub, 4))
            {
                op = pmdb_op_read;
            }
            else if (!strncmp("lookup", sub, 6))
            {
                op = pmdb_op_lookup;
            }
            else if (!strncmp("write:", sub, 6))
            {
                op = pmdb_op_write;
                write_seqno = strtoull(&sub[6], NULL, 10);
            }
            else
            {
                return -EBADMSG; // this should never happen (regex failed..)
            }
            break;
        case 2:
            op_cnt = strtoull(sub, NULL, 10);
            break;
        default:
            break;
        }
    }

    SIMPLE_LOG_MSG(LL_NOTIFY,
                   RAFT_NET_CLIENT_USER_ID_FMT
                   " op=%s seqno=%ld rc=%d op_cnt=%zu",
                   RAFT_NET_CLIENT_USER_ID_FMT_ARGS(&rncui, uuid_str, 0),
                   pmdp_op_2_string(op), write_seqno, rc, op_cnt);

    // Return errors here so they may be placed into the ctl-interface OUTFILE.
    return pmdbtc_queue_request(&rncui, op, op_cnt, write_seqno);
}

static util_thread_ctx_reg_int_t
pmdbtc_lreg_cb(enum lreg_node_cb_ops op, struct lreg_value *lv, void *arg)
{
    int rc = 0;

    (void)arg;

    if (!lv)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= PMDB_TEST_CLIENT_LREG__MAX)
        return -ERANGE;

    switch (op)
    {
    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;
        strncpy(lv->lrv_key_string, "pmdb-test-client", LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case PMDB_TEST_CLIENT_LREG_SUB_APP:
            lreg_value_fill_varray(lv, "pmdb-test-apps",
                                   LREG_USER_TYPE_RAFT_CLIENT_APP_DATA,
                                   pmdbtcNumApps,
                                   pmdbtc_test_apps_varray_lreg_cb);
            break;
        case PMDB_TEST_CLIENT_LREG_REQ_HISTORY:
            lreg_value_fill_varray(lv, "pmdb-request-history",
                                   LREG_USER_TYPE_RAFT_CLIENT_APP_DATA,
                                   (pmdbtcHistoryCnt %
                                    PMDB_TEST_CLIENT_REQ_HIST_SZ),
                                   pmdbtc_request_history_varray_lreg_cb);
            break;
        case PMDB_TEST_CLIENT_LREG_CMD_INPUT:
            lreg_value_fill_string(lv, "input",
                                   "apply pmdb test commands here");
            break;
        default:
            break;
        }
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case PMDB_TEST_CLIENT_LREG_CMD_INPUT:
            if (lv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
                return -EINVAL;

            rc = pmdbtc_parse_and_process_input_cmd(LREG_VALUE_TO_IN_STR(lv));
            break;
        default:
            rc = -EOPNOTSUPP;
            break;
        }
        break;

    default:
        rc = -EOPNOTSUPP;
        break;
    }

    return rc;
}

static void
pmdbtc_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s [-a (use async requests)] -r <UUID> -u <UUID>\n",
            argv[0]);

    exit(error);
}

static void
pmdbtc_getopt(int argc, char **argv)
{
    if (!argc || !argv)
        return;

    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'a':
            use_async_requests = true;
            break;
        case 'r':
            raft_uuid_str = optarg;
            break;
        case 'u':
            my_uuid_str = optarg;
            break;
        case 'h':
            pmdbtc_print_help(0, argv);
            break;
        default:
            pmdbtc_print_help(EINVAL, argv);
            break;
        }
    }

    if (!raft_uuid_str || !my_uuid_str)
        pmdbtc_print_help(EINVAL, argv);
}

int
main(int argc, char **argv)
{
    pmdbtc_getopt(argc, argv);

    pmdbtcPMDB = PmdbClientStart(raft_uuid_str, my_uuid_str);
    if (!pmdbtcPMDB)
        exit(-errno);

    /* Install after PmdbClientStart() so that requests cannot arrive via the
     * ctl-interface prior to initializaton.
     */
    LREG_ROOT_OBJECT_ENTRY_INSTALL(pumice_db_test_client);

    while (1) // Wait forever
        sleep(1);

    return 0;
}

static unsigned int
pmdbtc_app_random_get(struct pmdbtc_app *papp)
{
    unsigned result;

    if (random_r(&papp->papp_random_data, (int *)&result))
        SIMPLE_LOG_MSG(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
}

static void
pmdbtc_app_rtv_increment(struct pmdbtc_app *papp)
{
    NIOVA_ASSERT(papp);

    papp->papp_sync = 0;

    const uint32_t next_seq = pmdbtc_app_random_get(papp);
    const uint64_t new_sum =
        papp->papp_rtv.rtv_reply_xor_all_values ^ next_seq;

    SIMPLE_LOG_MSG(LL_DEBUG, "old-seqno=%lu val=%u sum[old:new]=%lu:%lu",
                   papp->papp_rtv.rtv_seqno, next_seq,
                   papp->papp_rtv.rtv_reply_xor_all_values, new_sum);

    papp->papp_rtv.rtv_seqno++;
    papp->papp_rtv.rtv_reply_xor_all_values = new_sum;

    // Store the value for this seqno so it may be used in a future request
    papp->papp_last_rtv_request.rtv_seqno = papp->papp_rtv.rtv_seqno;
    papp->papp_last_rtv_request.rtv_request_value = next_seq;
}

static void
pmdbtc_app_rtv_fast_forward(struct pmdbtc_app *papp, int64_t write_seqno)
{
    NIOVA_ASSERT(papp && write_seqno >= 0);
    papp->papp_sync = 0;

    pmdbtc_app_rand_init(papp);

    for (int64_t i = -1UL; i < write_seqno; i++)
        pmdbtc_app_rtv_increment(papp);
}

static void
pmdbtc_write_prep(struct pmdbtc_request *preq)
{
    NIOVA_ASSERT(preq && preq->preq_papp);

    struct pmdbtc_app *papp = preq->preq_papp;

    NIOVA_ASSERT(!raft_net_client_user_id_cmp(&papp->papp_rncui,
                                              &preq->preq_rncui));

    uuid_copy(preq->preq_rtdb.rtdb_client_uuid,
              RAFT_NET_CLIENT_USER_ID_2_UUID(&papp->papp_rncui, 0, 0));

    preq->preq_rtdb.rtdb_op = RAFT_TEST_DATA_OP_WRITE;
    preq->preq_rtdb.rtdb_num_values = 1;

    preq->preq_write_seqno == papp->papp_rtv.rtv_seqno ?
    pmdbtc_app_rtv_increment(papp) :
    pmdbtc_app_rtv_fast_forward(papp, preq->preq_write_seqno);

    preq->preq_rtv[0] = papp->papp_last_rtv_request;
}

static void
pmdbtc_result_capture(struct pmdbtc_request *preq, int rc)
{
    NIOVA_ASSERT(preq && preq->preq_papp);

    struct pmdbtc_app *papp = preq->preq_papp;

    papp->papp_last_tag = preq->preq_last_tag;
    papp->papp_last_request = preq->preq_submitted;
    papp->papp_obj_stat = preq->preq_obj_stat;
    papp->papp_obj_stat.status = -ABS(rc);
}

static void
pmdbtc_read_result_capture(struct pmdbtc_request *preq, int rc)
{
    if (!preq || !preq->preq_papp ||
        preq->preq_rtdb.rtdb_values[0].rtv_seqno < 0)
        return;

    struct pmdbtc_app *papp = preq->preq_papp;
    struct pmdbtc_app tmp_papp = *papp;

    pmdbtc_result_capture(preq, rc);
    if (rc)
        return;

    struct raft_test_values result_rtv = preq->preq_rtdb.rtdb_values[0];

    pmdbtc_app_rtv_fast_forward(&tmp_papp, result_rtv.rtv_seqno - 1);

    if (tmp_papp.papp_rtv.rtv_reply_xor_all_values ==
        result_rtv.rtv_reply_xor_all_values)
        papp->papp_last_rtv_validated = result_rtv;
    else
        papp->papp_obj_stat.status = -EBADF;
}

static void
pmdbtc_write_result_capture(struct pmdbtc_request *preq, int rc)
{
    if (!preq || !preq->preq_papp ||
        preq->preq_rtdb.rtdb_values[0].rtv_seqno < 0)
        return;

    struct pmdbtc_app *papp = preq->preq_papp;

    pmdbtc_result_capture(preq, rc);

    if (papp->papp_obj_stat.status)
        papp->papp_sync = 0;
    else if (rc)
        papp->papp_sync = 0;
    else
        papp->papp_sync = 1;
}

static void
pmdbtc_request_complete(struct pmdbtc_request *preq, int rc)
{
    // Makes a copy of the preq contents
    pmdbtc_app_history_add(preq);

    preq->preq_op_cnt--;
    if (!rc && preq->preq_op_cnt > 0)
    {
        preq->preq_write_seqno++;

        if (use_async_requests)
        {
            // Move request to the worker thread
            NIOVA_SET_COND_AND_WAKE(
                signal, {STAILQ_INSERT_TAIL(&preqQueue, preq, preq_lentry);},
                &mutex, &cond);
        }
        else
        {
            // Reinsert the operation onto the work queue.
            niova_mutex_lock(&mutex);
            STAILQ_INSERT_TAIL(&preqQueue, preq, preq_lentry);
            niova_mutex_unlock(&mutex);
        }
    }
    else
    {
        niova_free(preq);
    }
}

static void
pmdbtc_async_cb(void *arg, ssize_t rrc)
{
    if (!arg)
        return;

    struct pmdbtc_request *preq = (struct pmdbtc_request *)arg;
    pmdbtc_request_complete(preq, rrc);
}

static int
pmdbtc_submit_request(struct pmdbtc_request *preq)
{
    if (!preq || !preq->preq_papp)
        return -EINVAL;

    pmdb_obj_id_t *obj_id = (pmdb_obj_id_t *)&preq->preq_rncui.rncui_key;
    int rc = -EOPNOTSUPP;

    preq->preq_obj_stat.sequence_num = preq->preq_write_seqno;

    // Underhanded way to set rpc-user-tag in raft-client-rpc msg
    preq->preq_obj_stat.status = random_get();
    preq->preq_last_tag = preq->preq_obj_stat.status;

    switch (preq->preq_op)
    {
    case pmdb_op_lookup:
        if (!use_async_requests)
        {
            rc = PmdbObjLookup(pmdbtcPMDB, obj_id, &preq->preq_obj_stat);
            pmdbtc_result_capture(preq, rc);
        }
        else
        {
            rc = PmdbObjLookupNB(pmdbtcPMDB, obj_id, &preq->preq_obj_stat,
                                 pmdbtc_async_cb, preq);
        }
        break;
    case pmdb_op_read:
        if (!use_async_requests)
        {
            rc = PmdbObjGetX(pmdbtcPMDB, obj_id, NULL, 0,
                             (char *)&preq->preq_rtdb,
                             (sizeof(struct raft_test_data_block) +
                              sizeof(struct raft_test_values)),
                             &preq->preq_obj_stat);
            pmdbtc_read_result_capture(preq, rc);
        }
        else
        {
            rc = PmdbObjGetXNB(pmdbtcPMDB, obj_id, NULL, 0,
                               (char *)&preq->preq_rtdb,
                               (sizeof(struct raft_test_data_block) +
                                sizeof(struct raft_test_values)),
                               pmdbtc_async_cb, preq, &preq->preq_obj_stat);
        }
        break;
    case pmdb_op_write:
        pmdbtc_write_prep(preq);
        if (!use_async_requests)
        {
            rc = PmdbObjPut(pmdbtcPMDB, obj_id, (char *)&preq->preq_rtdb,
                            (sizeof(struct raft_test_data_block) +
                             sizeof(struct raft_test_values)),
                            &preq->preq_obj_stat);
            pmdbtc_write_result_capture(preq, rc);
        }
        else
        {
            rc = PmdbObjPutNB(pmdbtcPMDB, obj_id, (char *)&preq->preq_rtdb,
                              (sizeof(struct raft_test_data_block) +
                               sizeof(struct raft_test_values)),
                              pmdbtc_async_cb, preq, &preq->preq_obj_stat);
        }
        break;
    default:
        break;
    }

    if (!use_async_requests)
        pmdbtc_request_complete(preq, rc);

    return rc;
}

static void
pmdbtc_dequeue_request(void)
{
    struct pmdbtc_request *preq;

    niova_mutex_lock(&mutex);
    preq = STAILQ_FIRST(&preqQueue);
    if (preq)
        STAILQ_REMOVE_HEAD(&preqQueue, preq_lentry);

    niova_mutex_unlock(&mutex);

    if (preq)
        pmdbtc_submit_request(preq);
}

static void *
pmdb_test_client_worker(void *arg)
{
    struct thread_ctl *tc = arg;

    const int nsecs_wait = 1;

    THREAD_LOOP_WITH_CTL(tc)
    {
        struct timespec ts;
        niova_realtime_coarse_clock(&ts);
        ts.tv_sec += nsecs_wait;

        int rc = NIOVA_TIMEDWAIT_COND(!STAILQ_EMPTY(&preqQueue), &mutex, &cond,
                                      &ts);
        if (!rc)
            pmdbtc_dequeue_request();
    }

    return (void *)0;
}

static init_ctx_t NIOVA_CONSTRUCTOR(RAFT_CLIENT_CTOR_PRIORITY)
pmdbtc_init(void)
{
    FUNC_ENTRY(LL_NOTIFY);

    int rc = regcomp(&pmdbtcCmdRegex, PMDB_TEST_CLIENT_APPLY_CMD_REGEX, 0);
    NIOVA_ASSERT(!rc);

    rc = thread_create_watched(pmdb_test_client_worker, &pmdbtcThrCtl,
                               "pmdbtc-worker", NULL, NULL);
    NIOVA_ASSERT(!rc);

    thread_ctl_run(&pmdbtcThrCtl);

    return;
}

static destroy_ctx_t NIOVA_DESTRUCTOR(RAFT_CLIENT_CTOR_PRIORITY)
pmdbtc_destroy(void)
{
    thread_halt_and_destroy(&pmdbtcThrCtl);

    regfree(&pmdbtcCmdRegex);
}
