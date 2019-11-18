/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>

#include "env.h"
#include "ctor.h"
#include "log.h"

enum log_level dbgLevel = LL_WARN;

LREG_ROOT_ENTRY_GENERATE(log_entry_map, LREG_USER_TYPE_LOG_file);

enum log_lreg_function_entry_values
{
    LOG_LREG_ENTRY_LEVEL = 0,
    LOG_LREG_ENTRY_LINENO,
    LOG_LREG_ENTRY_EXEC_CNT,
    LOG_LREG_ENTRY_FUNC,
    LOG_LREG_ENTRY_MAX,
};

enum log_lreg_file_entry_values
{
    LOG_LREG_FILE_NAME = 0,
    LOG_LREG_FILE_LEVEL,
    LOG_LREG_FILE_LOG_ENTRIES,
    LOG_LREG_FILE_MAX,
};

static void
log_lreg_file_entry_multi_facet_value_cb(enum lreg_node_cb_ops op,
                                         struct log_entry_info *lei,
                                         struct lreg_value *lreg_val)
{
    if (lreg_val->lrv_value_idx_in > LOG_LREG_FILE_MAX)
        return;

    if (op == LREG_NODE_CB_OP_WRITE_VAL)
    {
        if (lreg_val->lrv_value_idx_in == LOG_LREG_FILE_LEVEL &&
            log_level_is_valid(lreg_val->put.lrv_value_in.lrv_unsigned_val))
        {
            enum log_level lvl = lreg_val->put.lrv_value_in.lrv_unsigned_val;

            lei->lei_level = lvl;
        }
    }
    else if (op == LREG_NODE_CB_OP_READ_VAL)
    {
        switch (lreg_val->lrv_value_idx_in)
        {
        case LOG_LREG_FILE_NAME:
            snprintf(lreg_val->lrv_key_string, LREG_VALUE_STRING_MAX,
                     "file-name");

            strncpy(LREG_VALUE_TO_OUT_STR(lreg_val), lei->lei_file,
                    LREG_VALUE_STRING_MAX);

            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_STRING;
            break;

        case LOG_LREG_FILE_LEVEL:
            strncpy(lreg_val->lrv_key_string, "file-log-level",
                    LREG_VALUE_STRING_MAX);

            snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                     "%s", ll_to_string(lei->lei_level));

            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_STRING;

            break;

        case LOG_LREG_FILE_LOG_ENTRIES:
            snprintf(lreg_val->lrv_key_string, LREG_VALUE_STRING_MAX,
                     "log-entries");

            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_ARRAY;
            break;

        default:
            break;
        }
    }
}

static void
log_lreg_function_entry_multi_facet_value_cb(enum lreg_node_cb_ops op,
                                             struct log_entry_info *lei,
                                             struct lreg_value *lreg_val)
{
    if (lreg_val->lrv_value_idx_in > LOG_LREG_ENTRY_MAX)
        return;

    if (op == LREG_NODE_CB_OP_WRITE_VAL)
    {
        if (lreg_val->lrv_value_idx_in == LOG_LREG_ENTRY_LEVEL &&
            log_level_is_valid(lreg_val->put.lrv_value_in.lrv_unsigned_val))
        {
            enum log_level lvl = lreg_val->put.lrv_value_in.lrv_unsigned_val;

            lei->lei_level = lvl;
        }
    }
    else if (op == LREG_NODE_CB_OP_READ_VAL)
    {
        switch (lreg_val->lrv_value_idx_in)
        {
        case LOG_LREG_ENTRY_LEVEL:
            strncpy(lreg_val->lrv_key_string, "log-level",
                    LREG_VALUE_STRING_MAX);

            snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                     "%s", ll_to_string(lei->lei_level));

            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_STRING;

            break;

        case LOG_LREG_ENTRY_LINENO:
            strncpy(lreg_val->lrv_key_string, "line-number",
                    LREG_VALUE_STRING_MAX);
            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_UNSIGNED_VAL;
            lreg_val->get.lrv_value_out.lrv_unsigned_val = lei->lei_lineno;
            break;

        case LOG_LREG_ENTRY_EXEC_CNT:
            strncpy(lreg_val->lrv_key_string, "exec-cnt",
                    LREG_VALUE_STRING_MAX);
            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_UNSIGNED_VAL;
            lreg_val->get.lrv_value_out.lrv_unsigned_val = lei->lei_exec_cnt;
            break;

        case LOG_LREG_ENTRY_FUNC:
            strncpy(lreg_val->lrv_key_string, "function",
                    LREG_VALUE_STRING_MAX);

            snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                     "%s", lei->lei_func);

            lreg_val->get.lrv_request_type_out = LREG_NODE_TYPE_STRING;
            break;
        default:
            break;
        }
    }
}

static lreg_install_int_ctx_t
log_lreg_function_entry_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                           struct lreg_value *lreg_val)
{
    struct log_entry_info *lei = lrn->lrn_cb_arg;

    if (lreg_val)
        lreg_val->get.lrv_num_keys_out = LOG_LREG_ENTRY_MAX;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lreg_val)
            return -EINVAL;

        snprintf(lreg_val->lrv_key_string, LREG_VALUE_STRING_MAX,
                 "log-entry-name");

        snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                 "%s:%d", lei->lei_func, (int)lei->lei_lineno);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        if (!lreg_val)
            return -EINVAL;

        log_lreg_function_entry_multi_facet_value_cb(op, lei, lreg_val);
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
    if (lreg_val)
        lreg_val->get.lrv_num_keys_out = LOG_LREG_FILE_MAX;

    struct log_entry_info *lei = lrn->lrn_cb_arg;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lreg_val)
            return -EINVAL;

        strncpy(LREG_VALUE_TO_OUT_STR(lreg_val), lei->lei_file,
                LREG_VALUE_STRING_MAX);

        break;

    case LREG_NODE_CB_OP_READ_VAL:
        if (!lreg_val)
            return -EINVAL;

        log_lreg_file_entry_multi_facet_value_cb(op, lei, lreg_val);
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
    NIOVA_ASSERT(lrn->lrn_user_type == LREG_USER_TYPE_LOG_file ||
                 lrn->lrn_user_type == LREG_USER_TYPE_LOG_func);

    /* NOTE: lrv_request_type_out may be overridden later in the stack.
     */
    if (lreg_val)
        lreg_val->get.lrv_request_type_out = lrn->lrn_node_type;

    return lrn->lrn_user_type == LREG_USER_TYPE_LOG_file ?
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
    const struct niova_env_var *ev = env_get(NIOVA_ENV_VAR_log_level);

    if (ev && ev->nev_present)
        log_level_set(ev->nev_long_value);

    LREG_ROOT_ENTRY_INSTALL(log_entry_map);
    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
};

destroy_ctx_t
log_subsys_destroy(void)
{
    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye");
};
