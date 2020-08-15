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
#include "pumice_db_client.h"
#include "regex_defines.h"
#include "thread.h"
#include "util_thread.h"

#define OPTS "au:r:h"

#define PMDB_TEST_CLIENT_MAX_RNCUI 128
static struct raft_net_client_user_id pmdbtcRncui[PMDB_TEST_CLIENT_MAX_RNCUI];
static unsigned int pmdbtcNumRncui;

static regex_t pmdbtcCmdRegex;

static const char *raft_uuid_str;
static const char *my_uuid_str;
static bool  use_async_requests = false;


static pmdb_t pmdbtcPMDB;

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

enum pmdb_lreg_values
{
    PMDB_TEST_CLIENT_LREG_SUB_APP,
    PMDB_TEST_CLIENT_LREG_CMD_INPUT,
    PMDB_TEST_CLIENT_LREG__MAX,
};

REGISTRY_ENTRY_FILE_GENERATE;

struct pmdbtc_request
{
    struct raft_net_client_user_id preq_rncui;
    enum PmdbOpType                preq_op;
    size_t                         preq_op_cnt;
    int64_t                        preq_write_seqno;
    STAILQ_ENTRY(pmdbtc_request)   preq_lentry;
    size_t                         preq_reply_data_size;
    PmdbMsg_t                      preq_reply; // must be last
};

STAILQ_HEAD(pmdbtc_request_queue, pmdbtc_request);
static struct pmdbtc_request_queue preq_queue =
    STAILQ_HEAD_INITIALIZER(preq_queue);

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
    return 0;
}


static util_thread_ctx_reg_int_t
pmdbtc_queue_request(const struct pmdbtc_request *preq)
{
    if (!preq)
        return -EINVAL;

    else if (preq->preq_op != pmdb_op_lookup ||
             preq->preq_op != pmdb_op_read   ||
             preq->preq_op != pmdb_op_write)
        return -EOPNOTSUPP;

    struct pmdbtc_request *new_preq =
        niova_malloc_can_fail(sizeof(struct pmdbtc_request) +
                              preq->preq_reply_data_size);

    if (!new_preq)
        return -ENOMEM;

    memcpy(new_preq, preq, sizeof(struct pmdbtc_request));

    new_preq->preq_reply.pmdbrm_data_size = preq->preq_reply_data_size;

    int rc = 0;

    if (!use_async_requests)
    {
        // Move request to the worker thread
        NIOVA_SET_COND_AND_WAKE(
            signal,
            {STAILQ_INSERT_TAIL(&preq_queue, new_preq, preq_lentry);},
            &mutex, &cond);
    }
    else
    {
        switch (preq->preq_op)
        {
        case pmdb_op_lookup:

        }
    }


    return rc;
}

static util_thread_ctx_reg_int_t
pmdbtc_parse_and_process_input_cmd(const char *input_cmd_str)
{
    if (!input_cmd_str)
        return -EINVAL;

    int rc = regexec(&pmdbtcCmdRegex, input_cmd_str, 0, NULL, 0);

    SIMPLE_LOG_MSG(LL_DEBUG, "input=%s (regex-rc=%d)", input_cmd_str, rc);

    if (rc)
        return -EBADMSG;

    struct raft_net_client_user_id rncui = {0};

    struct pmdbtc_request pr = {0};

    char local_str[LREG_VALUE_STRING_MAX + 1] = {0};
    strncpy(local_str, input_cmd_str, LREG_VALUE_STRING_MAX);

    int64_t write_seqno = -1ULL;
    bool write_op = false;
    char *uuid_str = NULL;

    enum PmdbOpType op = pmdb_op_any;

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
            rc = raft_net_client_user_id_parse(sub, &pr.preq_rncui, 0);
            if (rc)
                return -EBADMSG;

            uuid_str = sub;
            uuid_str[UUID_STR_LEN - 1] = '\0';

            break;
        case 1:
            if (!strncmp("read", sub, 4))
                pr.preq_op = pmdb_op_read;
            else if (!strncmp("lookup", sub, 6))
                pr.preq_op = pmdb_op_lookup;
            else if (!strncmp("write", sub, 5))
                pr.preq_op = pmdb_op_write;
            else
                return -EBADMSG; // this should never happen (regex failed..)
            break;
        case 2:
            NIOVA_ASSERT(pr.preq_op == pmdb_op_write);
            pr.preq_write_seqno = strtoull(sub, NULL, 10);
            break;
        default:
            break;
        }
    }

    SIMPLE_LOG_MSG(LL_DEBUG,
                   RAFT_NET_CLIENT_USER_ID_FMT" op=%s seqno=%ld rc=%d",
                   RAFT_NET_CLIENT_USER_ID_FMT_ARGS(&pr.preq_rncui, uuid_str,
                                                    0),
                   pmdp_op_2_string(pr.preq_op, pr.preq_write_seqno, rc);

    // Return errors here so they may be placed into the ctl-interface OUTFILE.
    return pmdbtc_queue_request(&pr);
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
                                   pmdbtcNumRncui,
                                   pmdbtc_test_apps_varray_lreg_cb);
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

    sleep(12000);
    return 0;
}

static init_ctx_t NIOVA_CONSTRUCTOR(RAFT_CLIENT_CTOR_PRIORITY)
pmdbtc_init(void)
{
    FUNC_ENTRY(LL_NOTIFY);

    int rc = regcomp(&pmdbtcCmdRegex, PMDB_TEST_CLIENT_APPLY_CMD_REGEX, 0);
    NIOVA_ASSERT(!rc);

    return;
}

static destroy_ctx_t NIOVA_DESTRUCTOR(RAFT_CLIENT_CTOR_PRIORITY)
pmdbtc_destroy(void)
{
    regfree(&pmdbtcCmdRegex);
}
