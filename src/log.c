/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>

#include "ctor.h"
#include "log.h"

enum log_level dbgLevel = LL_WARN;

LREG_ROOT_ENTRY_GENERATE(logEntries, LREG_USER_TYPE_LOG);

static lreg_install_int_ctx_t
log_lreg_function_entry_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                           struct lreg_value *lreg_val)
{
    struct log_entry_info *lei = lrn->lrn_cb_arg;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                 "%s:%d", lei->lei_func, (int)lei->lei_lineno);
        break;
    case LREG_NODE_CB_OP_READ_VAL:
        lreg_val->get.lrv_value_out.lrv_unsigned_val = lei->lei_level;
        break;
    case LREG_NODE_CB_OP_WRITE_VAL:
        lei->lei_level = lreg_val->put.lrv_value_in.lrv_unsigned_val;
        break;
    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;
    default:
        return -ENOENT;
    }

    return 0;
}

static lreg_install_int_ctx_t
log_lreg_file_entry_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                       struct lreg_value *lreg_val)
{
    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        strncpy(LREG_VALUE_TO_OUT_STR(lreg_val), lrn->lrn_cb_arg,
                LREG_VALUE_STRING_MAX);
        break;
    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;
    default:
        return -ENOENT;
    }

    return 0;
}

lreg_install_int_ctx_t
log_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
            struct lreg_value *lreg_val)
{
    NIOVA_ASSERT(lreg_statically_allocated_node_check(lrn));
    NIOVA_ASSERT(lrn->lrn_user_type == LREG_USER_TYPE_LOG);

    return lrn->lrn_node_type == LREG_NODE_TYPE_ARRAY ?
        log_lreg_file_entry_cb(op, lrn, lreg_val) :
        log_lreg_function_entry_cb(op, lrn, lreg_val);
}

void
log_level_set(enum log_level ll)
{
    dbgLevel = ll;
}

init_ctx_t
log_subsys_init(void)
{
    LREG_ROOT_ENTRY_INSTALL(logEntries);
    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
};
