/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <unistd.h>
#include <uuid/uuid.h>

#include "common.h"
#include "util_thread.h"
#include "pumice_db_client.h"

#define OPTS "u:r:h"

const char *raft_uuid_str;
const char *my_uuid_str;

enum pmdb_lreg_values
{
    PMDB_TEST_CLIENT_LREG_SUB_APP = 0,
    PMDB_TEST_CLIENT_LREG__MAX,
};

#define PMDB_TEST_CLIENT_MAX_RNCUI 1024
struct raft_net_client_user_id rncui[PMDB_TEST_CLIENT_MAX_RNCUI];
unsigned int numRncui;

static util_thread_ctx_reg_int_t
pumice_db_test_client_multi_facet_cb(enum lreg_node_cb_ops,
                                     struct lreg_value *, void *);

LREG_ROOT_ENTRY_GENERATE_OBJECT(pumice_db_test_client,
                                LREG_USER_TYPE_RAFT_CLIENT_APP,
                                PMDB_TEST_CLIENT_LREG__MAX,
                                pumice_db_test_client_multi_facet_cb, NULL);

static util_thread_ctx_reg_int_t
pumice_db_test_client_multi_facet_cb(enum lreg_node_cb_ops op,
                                     struct lreg_value *lv, void *arg)
{
    return 0;
}


static util_thread_ctx_reg_int_t
pmdbtc_test_apps_varray_lreg_cb(enum lreg_node_cb_ops op,
                                struct lreg_node *lrn, struct lreg_value *lv)
{
    return 0;
}

static util_thread_ctx_reg_int_t
pmdbtc_parse_and_process_input_cmd(const char *input_cmd_str)
{
    if (!input_cmd_str)
        return -EINVAL;

    return 0;
}

static util_thread_ctx_reg_int_t
pmdbtc_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
               struct lreg_value *lv)
{
    int rc = 0;

    if (!lrn || !lv)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= PMDB_TEST_CLIENT_LREG__MAX)
        return -ERANGE;

    // Only one value is supported here
    else if (lv->lrv_value_idx_in != PMDB_TEST_CLIENT_LREG_SUB_APP)
        return -EOPNOTSUPP;

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
        lreg_value_fill_varray(lv, "pmdb-test-apps",
                               LREG_USER_TYPE_RAFT_CLIENT_APP_DATA,
                               numRncui, pmdbtc_test_apps_varray_lreg_cb);
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
        if (lv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        rc = pmdbtc_parse_and_process_input_cmd(LREG_VALUE_TO_IN_STR(lv));

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
            "Usage: %s -r <UUID> -u <UUID>\n", argv[0]);

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

    pmdb_t pmdb = PmdbClientStart(raft_uuid_str, my_uuid_str);
    if (!pmdb)
        exit(-errno);

    sleep(12000);
    return 0;
}
