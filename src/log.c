/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>

#include "niova/env.h"
#include "niova/ctor.h"
#include "niova/log.h"

REGISTRY_ENTRY_FILE_GENERATE;

static const enum log_level defaultMasterLogLevel = LL_WARN;
static enum log_level masterLogLevel = LL_WARN;

enum log_subsystem_keys
{
    LOG_SUBSYS_KEY__MIN             = 0,
    LOG_SUBSYS_KEY_MASTER_LOG_LEVEL = 0,
    LOG_SUBSYS_KEY_DATE_FORMAT      = 1,
    LOG_SUBSYS_KEY__MAX             = 2,
};

enum log_lreg_function_entry_values
{
    LOG_LREG_ENTRY_LEVEL,
    LOG_LREG_ENTRY_LINENO,
    LOG_LREG_ENTRY_EXEC_CNT,
    LOG_LREG_ENTRY_FUNC,
    LOG_LREG_ENTRY_TAG,
    LOG_LREG_ENTRY_MAX,
};

enum log_lreg_file_entry_values
{
    LOG_LREG_FILE_NAME,
    LOG_LREG_FILE_LEVEL,
    LOG_LREG_FILE_LOG_ENTRIES,
    LOG_LREG_FILE_MAX,
};

// Forward declaration for log_subsystem lreg entry.
static int //util_thread_ctx_reg_int_t
log_subsys_lreg_multi_facet_cb(enum lreg_node_cb_ops,
                               struct lreg_value *, void *);

// LREG array for file entries (functions are children of their file)
LREG_ROOT_ENTRY_GENERATE(log_entry_map, LREG_USER_TYPE_LOG_file);

LREG_ROOT_ENTRY_GENERATE_OBJECT(log_subsystem, LREG_USER_TYPE_LOG_subsys,
                                LOG_SUBSYS_KEY__MAX,
                                log_subsys_lreg_multi_facet_cb, NULL,
                                LREG_INIT_OPT_NONE);

static void
log_lreg_check_and_assign_log_level(struct log_entry_info *lei,
                                    const struct lreg_value *lreg_val)
{
    if (!lei || !lreg_val ||
        (lreg_val->lrv_value_idx_in != LOG_LREG_ENTRY_LEVEL &&
         lreg_val->lrv_value_idx_in != LOG_LREG_FILE_LEVEL))
        return;

    enum log_level lvl = LL_ANY;

    if (lreg_in_value_is_numeric(lreg_val))
        lvl = lreg_val->put.lrv_value_in.lrv_unsigned_val;

    else if (LREG_VALUE_TO_REQ_TYPE_IN(lreg_val) == LREG_VAL_TYPE_STRING)
        lvl = ll_from_string(LREG_VALUE_TO_IN_STR(lreg_val));

    if ((log_level_is_valid(lvl) && lvl > LL_FATAL) ||
        lvl == LL_ANY) // LL_ANY restores to 'default' user-supplied value
    {
        LOG_MSG(LL_DEBUG, "%s log level from %s to %s",
                lei->lei_func, ll_to_string(lei->lei_level),
                ll_to_string(lvl));
        lei->lei_level = lvl;
    }
    else
    {
        LOG_MSG(LL_DEBUG, "failed to %s log level from %s to %s",
                lei->lei_func, ll_to_string(lei->lei_level),
                ll_to_string(lvl));
    }
}

static int //util_thread_ctx_reg_int_t
log_subsys_lreg_multi_facet_cb(enum lreg_node_cb_ops op,
                               struct lreg_value *lv, void *unused_arg)
{
    if (!lv || unused_arg)  //'unused_arg' should be NULL
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= LOG_SUBSYS_KEY__MAX)
        return -ERANGE;

    else if (op != LREG_NODE_CB_OP_READ_VAL && op != LREG_NODE_CB_OP_WRITE_VAL)
        return -EOPNOTSUPP;

    int rc = 0;

    if (op == LREG_NODE_CB_OP_WRITE_VAL)
    {
        if (lv->lrv_value_idx_in == LOG_SUBSYS_KEY_MASTER_LOG_LEVEL)
        {
            struct log_entry_info lei = {.lei_level = log_level_get(),
                                         .lei_func = __func__};

            log_lreg_check_and_assign_log_level(&lei, lv);

            if (lei.lei_level != log_level_get())
            {
                if (log_level_is_valid(lei.lei_level))
                    log_level_set(lei.lei_level);

                else if (lei.lei_level == LL_ANY) // reset to default
                    log_level_set(defaultMasterLogLevel);
            }
        }
        else
        {
            rc = -EPERM;
        }
    }
    else if (op == LREG_NODE_CB_OP_READ_VAL)
    {
        if (lv->lrv_value_idx_in == LOG_SUBSYS_KEY_MASTER_LOG_LEVEL)
            lreg_value_fill_string(lv, "master_log_level",
                                   ll_to_string(log_level_get()));
        else if (lv->lrv_value_idx_in == LOG_SUBSYS_KEY_DATE_FORMAT)
            lreg_value_fill_string(lv, "log_msg_date_format", "%blah");
    }

    return rc;
}

static void
log_lreg_file_entry_multi_facet_value_cb(enum lreg_node_cb_ops op,
                                         struct log_entry_info *lei,
                                         struct lreg_value *lreg_val)
{
    if (lreg_val->lrv_value_idx_in > LOG_LREG_FILE_MAX)
        return;

    if (op == LREG_NODE_CB_OP_WRITE_VAL)
    {
        log_lreg_check_and_assign_log_level(lei, lreg_val);
    }
    else if (op == LREG_NODE_CB_OP_READ_VAL)
    {
        lreg_val->get.lrv_user_type_out = LREG_USER_TYPE_LOG_func;

        switch (lreg_val->lrv_value_idx_in)
        {
        case LOG_LREG_FILE_NAME:
            lreg_value_fill_string(lreg_val, "file_name", lei->lei_file);
            break;

        case LOG_LREG_FILE_LEVEL:
            lreg_value_fill_string(lreg_val, "file_log_level",
                                   ll_to_string(lei->lei_level));
            break;

        case LOG_LREG_FILE_LOG_ENTRIES:
            lreg_value_fill_array(lreg_val, "log_entries",
                                  LREG_USER_TYPE_LOG_func);
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
    if (lreg_val->lrv_value_idx_in >= LOG_LREG_ENTRY_MAX)
        return;

    if (op == LREG_NODE_CB_OP_WRITE_VAL)
    {
        log_lreg_check_and_assign_log_level(lei, lreg_val);
    }
    else if (op == LREG_NODE_CB_OP_READ_VAL)
    {
        switch (lreg_val->lrv_value_idx_in)
        {
        case LOG_LREG_ENTRY_LEVEL:
            lreg_value_fill_string(lreg_val, "log_level",
                                   ll_to_string(lei->lei_level));
            break;

        case LOG_LREG_ENTRY_LINENO:
            lreg_value_fill_unsigned(lreg_val, "line_number", lei->lei_lineno);
            break;

        case LOG_LREG_ENTRY_EXEC_CNT:
            lreg_value_fill_unsigned(lreg_val, "exec_cnt", lei->lei_exec_cnt);
            break;

        case LOG_LREG_ENTRY_FUNC:
            lreg_value_fill_string(lreg_val, "function", lei->lei_func);
            break;

        case LOG_LREG_ENTRY_TAG:
            lreg_value_fill_string(lreg_val, "tag", lei->lei_tag);
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

    // Don't modify lreg_val if the op is a write
    if (lreg_val && op == LREG_NODE_CB_OP_GET_NODE_INFO)
        lreg_val->get.lrv_num_keys_out = LOG_LREG_ENTRY_MAX;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lreg_val)
            return -EINVAL;

        snprintf(lreg_val->lrv_key_string, LREG_VALUE_STRING_MAX,
                 "log_entry_name");

        snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                 "%s:%d", lei->lei_func, (int)lei->lei_lineno);
        break;

    case LREG_NODE_CB_OP_READ_VAL: // fall through
    case LREG_NODE_CB_OP_WRITE_VAL:
        if (!lreg_val)
            return -EINVAL;

        log_lreg_function_entry_multi_facet_value_cb(op, lei, lreg_val);
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: // fall through
    case LREG_NODE_CB_OP_DESTROY_NODE: // fall through
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE:
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
    struct log_entry_info *lei = lrn->lrn_cb_arg;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NODE_INFO:
        if (!lreg_val)
            return -EINVAL;

        lreg_val->get.lrv_num_keys_out = LOG_LREG_FILE_MAX;

        snprintf(lreg_val->lrv_key_string, LREG_VALUE_STRING_MAX,
                 "log_file_entry_name");

        strncpy(LREG_VALUE_TO_OUT_STR(lreg_val), lei->lei_file,
                LREG_VALUE_STRING_MAX);

        break;

    case LREG_NODE_CB_OP_READ_VAL:  // fall through
    case LREG_NODE_CB_OP_WRITE_VAL:
        if (!lreg_val)
            return -EINVAL;

        log_lreg_file_entry_multi_facet_value_cb(op, lei, lreg_val);
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: // fall through
    case LREG_NODE_CB_OP_DESTROY_NODE: // fall through
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE:
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

    return lrn->lrn_user_type == LREG_USER_TYPE_LOG_file ?
        log_lreg_file_entry_cb(op, lrn, lreg_val) :
        log_lreg_function_entry_cb(op, lrn, lreg_val);
}

void
log_level_set(enum log_level ll)
{
    if (ll != masterLogLevel)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "master log-level changing from %s to %s",
                       ll_to_string(masterLogLevel), ll_to_string(ll));

        masterLogLevel = ll;
    }
}

inline enum log_level
log_level_get(void)
{
    return masterLogLevel;
}

init_ctx_t
log_subsys_init(void)
{
    masterLogLevel = defaultMasterLogLevel;

    const struct niova_env_var *ev = env_get(NIOVA_ENV_VAR_log_level);

    if (ev && ev->nev_present)
        log_level_set(ev->nev_long_value);

    LREG_ROOT_ENTRY_INSTALL(log_entry_map);
    LREG_ROOT_OBJECT_ENTRY_INSTALL(log_subsystem);
    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
};

destroy_ctx_t
log_subsys_destroy(void)
{
    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye");
};
