/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2020
 */

#include "ctor.h"
#include "fault_inject.h"
#include "log.h"
#include "registry.h"
#include "util_thread.h"

#ifdef NIOVA_FAULT_INJECTION_ENABLED
static const bool faultInjectionEnabled = true;
#else
static const bool faultInjectionEnabled = false;
#endif

static bool faultInjectionInit = false;

#define FAULT_INJECT_SET_SIZE_MAX 128

LREG_ROOT_ENTRY_GENERATE(fault_injection_points, LREG_USER_TYPE_FAULT_INJECT);

enum fault_inject_reg_keys
{
    FAULT_INJECT_REG_KEY_NAME,          //string
    FAULT_INJECT_REG_KEY_ENABLED,       //bool
    FAULT_INJECT_REG_KEY_FILE,          //string
    FAULT_INJECT_REG_KEY_FUNCTION,      //string
    FAULT_INJECT_REG_KEY_LINENO,        //unsigned
    FAULT_INJECT_REG_KEY_WHEN,          //string
    FAULT_INJECT_REG_KEY_LAST_INJECTED, //string
    FAULT_INJECT_REG_KEY_LAST_BYPASS,   //string
    FAULT_INJECT_REG_KEY_INJECTED_CNT,  //unsigned
    FAULT_INJECT_REG_KEY_FREQ_SECONDS,  //unsigned (short)
    FAULT_INJECT_REG_KEY_NUM_REMAINING, //unsigned
    FAULT_INJECT_REG_KEY_COND_EXEC_CNT, //unsigned
    FAULT_INJECT_REG_KEY__MAX,
};

struct fault_injection_set
{
    struct fault_injection *finj_set;
    size_t                  finj_set_size;
};

#define FAULT_INJECTION_SET_MAX 8

static struct fault_injection coreFaultInjections[] =
{
    [FAULT_INJECT_any] = {
        .flti_name = "any injection",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 1,
    },
    [FAULT_INJECT_async_raft_client_request_expire] = {
        .flti_name = "async_raft_client_request_expire",
        .flti_when = FAULT_INJECT_PERIOD_one_time_only,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_force_bulk_recovery] = {
        .flti_name = "raft_force_bulk_recovery",
        .flti_when = FAULT_INJECT_PERIOD_one_time_only,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_server_main_loop_break] = {
        .flti_name = "raft_server_main_loop_break",
        .flti_when = FAULT_INJECT_PERIOD_one_time_only,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_leader_may_be_deposed] = {
        .flti_name = "raft_leader_may_be_deposed",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_follower_ignores_AE] = {
        .flti_name = "raft_follower_ignores_non_hb_AE_request",
        .flti_when = FAULT_INJECT_PERIOD_every_time_unless_bypassed,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_client_recv_handler_bypass] = {
        .flti_name = "raft_client_recv_handler_bypass",
        .flti_when = FAULT_INJECT_PERIOD_every_time_unless_bypassed,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_client_recv_handler_process_reply_bypass] = {
        .flti_name = "raft_client_recv_handler_process_reply_bypass",
        .flti_when = FAULT_INJECT_PERIOD_every_time_unless_bypassed,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_candidate_state_disabled] = {
        .flti_name = "raft_candidate_state_disabled",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_server_bypass_sm_apply] = {
        .flti_name = "raft_server_bypass_sm_apply",
        .flti_when = FAULT_INJECT_PERIOD_every_time_unless_bypassed,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_pvc_becomes_candidate] = {
        .flti_name = "raft_pvc_becomes_candidate",
        .flti_when = FAULT_INJECT_PERIOD_one_time_only,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_disabled] = {
        .flti_name = "disabled injection",
        .flti_when = FAULT_INJECT_PERIOD_one_time_only,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_coalesced_writes] = {
        .flti_name = "coalesced_writes",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_ignore_einprogress] = {
        .flti_name = "ignore_einprogress",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_force_set_max_scan_entries] = {
        .flti_name = "raft_force_set_max_scan_entries",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_limit_rsync_bw] = {
        .flti_name = "raft_limit_rsync_bw",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_raft_leader_ignore_direct_req] = {
        .flti_name = "raft_leader_ignore_direct_req",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_pmdb_range_read_keep_snapshot] = {
        .flti_name = "pmdb_range_read_keep_snapshot",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
    [FAULT_INJECT_pmdb_range_read_keep_old_snapshot] = {
        .flti_name = "pmdb_range_read_keep_old_snapshot",
        .flti_when = FAULT_INJECT_PERIOD_every_time,
        .flti_enabled = 0,
    },
};

static struct fault_injection_set
faultInjectionSets[FAULT_INJECTION_SET_MAX] = {
    [0].finj_set = coreFaultInjections,
    [0].finj_set_size = ARRAY_SIZE(coreFaultInjections),
};
static size_t numFaultInjectionSets = 1;

struct fault_injection *
fault_injection_lookup(const size_t id, const int module)
{
    if (module >= FAULT_INJECTION_SET_MAX ||
        faultInjectionSets[module].finj_set == NULL ||
        id >= faultInjectionSets[module].finj_set_size)
        return NULL;

    if (faultInjectionSets[module].finj_set[id].flti_file)
        return NULL;               /* already installed        */

    return &faultInjectionSets[module].finj_set[id];
}

static util_thread_ctx_reg_int_t
fault_injection_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                        struct lreg_value *lrv)
{
    if (!lrn || (!lrv && (op == LREG_NODE_CB_OP_GET_NODE_INFO ||
                          op == LREG_NODE_CB_OP_READ_VAL ||
                          op == LREG_NODE_CB_OP_WRITE_VAL)))
        return -EINVAL;

    struct fault_injection *flti = OFFSET_CAST(fault_injection, flti_lrn, lrn);

    unsigned int tmp_val = 0;
    bool tmp_bool;
    int tmp_rc;

    switch (op)
    {
    case LREG_NODE_CB_OP_INSTALL_NODE: /* fall through */
    case LREG_NODE_CB_OP_DESTROY_NODE: /* fall through */
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE:
        break; // No-ops since these entries are effectively static
    case LREG_NODE_CB_OP_GET_NODE_INFO:
        lrv->get.lrv_num_keys_out = FAULT_INJECT_REG_KEY__MAX;
        break;
    case LREG_NODE_CB_OP_READ_VAL:
        switch (lrv->lrv_value_idx_in)
        {
        case FAULT_INJECT_REG_KEY_NAME:
            lreg_value_fill_string(lrv, "name", flti->flti_name);
            break;
        case FAULT_INJECT_REG_KEY_ENABLED:
            lreg_value_fill_bool(lrv, "enabled",
                                 flti->flti_enabled ? true : false);
            break;
        case FAULT_INJECT_REG_KEY_FILE:
            lreg_value_fill_string(lrv, "file", flti->flti_file);
            break;
        case FAULT_INJECT_REG_KEY_FUNCTION:
            lreg_value_fill_string(lrv, "function", flti->flti_func);
            break;
        case FAULT_INJECT_REG_KEY_LINENO:
            lreg_value_fill_unsigned(lrv, "line_number", flti->flti_lineno);
            break;
        case FAULT_INJECT_REG_KEY_WHEN:
            lreg_value_fill_string(lrv, "when_to_inject",
                                   fault_injection_when_2_str(flti));
            break;
        case FAULT_INJECT_REG_KEY_LAST_INJECTED:
            lreg_value_fill_string_time(lrv, "last_injected_at",
                                        flti->flti_last);
            break;
        case FAULT_INJECT_REG_KEY_LAST_BYPASS:
            lreg_value_fill_string_time(lrv, "last_bypassed_at",
                                        flti->flti_last_bypass);
            break;
        case FAULT_INJECT_REG_KEY_INJECTED_CNT:
            lreg_value_fill_unsigned(lrv, "injection_count",
                                     flti->flti_inject_cnt);
            break;
        case FAULT_INJECT_REG_KEY_FREQ_SECONDS:
            lreg_value_fill_unsigned(lrv, "frequency_seconds",
                                     flti->flti_freq_seconds);
            break;
        case FAULT_INJECT_REG_KEY_NUM_REMAINING:
            lreg_value_fill_unsigned(lrv, "num_remaining",
                                     flti->flti_num_remaining);
            break;
        case FAULT_INJECT_REG_KEY_COND_EXEC_CNT:
            lreg_value_fill_unsigned(lrv, "cond_exec_count",
                                     flti->flti_cond_exec_cnt);
            break;
        default:
            return -EOPNOTSUPP;
        }
        break;
    case LREG_NODE_CB_OP_WRITE_VAL:
        if (lrv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        switch (lrv->lrv_value_idx_in)
        {
        case FAULT_INJECT_REG_KEY_ENABLED:
            tmp_rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lrv),
                                          &tmp_bool);
            if (tmp_rc)
                return tmp_rc;

            flti->flti_enabled = tmp_bool;
            break;
        case FAULT_INJECT_REG_KEY_NUM_REMAINING:
            tmp_rc = niova_string_to_unsigned_int(LREG_VALUE_TO_IN_STR(lrv),
                                                  &tmp_val);
            if (tmp_rc)
                return tmp_rc;

            flti->flti_num_remaining = tmp_val;
            break;
        case FAULT_INJECT_REG_KEY_WHEN:          // fall through
        case FAULT_INJECT_REG_KEY_FREQ_SECONDS:  // fall through
            return -EPERM;
        default:
            return -EOPNOTSUPP;
        }
        break;
    default:
        break;
    }

    return 0;
}

static void
fault_inject_set_lreg_install(struct fault_injection_set *fis)
{
    NIOVA_ASSERT(fis);
    SIMPLE_LOG_MSG(LL_DEBUG, "size=%zu", fis->finj_set_size);

    for (size_t i = 0; i < fis->finj_set_size; i++)
    {
        struct lreg_node *lrn = &fis->finj_set[i].flti_lrn;

        lreg_node_init(lrn, LREG_USER_TYPE_FAULT_INJECT,
                       fault_injection_lreg_cb, NULL, LREG_INIT_OPT_NONE);

        int rc = lreg_node_install(
            lrn, LREG_ROOT_ENTRY_PTR(fault_injection_points));

        FATAL_IF(rc, "lreg_node_install() %s", strerror(-rc));
    }
}

// Append of 'false' will overwrite the first entry
int
fault_inject_set_install(struct fault_injection *finj_set, size_t set_size,
                         bool append)
{
//    if (!init_ctx())
//        return -EBUSY;

    if (!faultInjectionInit)
        return -ENODEV;

    if (!faultInjectionEnabled)
        return 0;

    if (!finj_set || !set_size)
        return -EINVAL;

    if (set_size > FAULT_INJECT_SET_SIZE_MAX)
        return -E2BIG;

    int idx = 0;

    // Note this does not support reuse of slots
    if (append)
    {
        for (; idx < FAULT_INJECTION_SET_MAX; idx++)
            if (faultInjectionSets[idx].finj_set == NULL)
                break;

        if (idx == FAULT_INJECTION_SET_MAX)
            return -ENOSPC;

        numFaultInjectionSets = idx + 1;
    }
    else // Remove the items residing at index@0
    {
        struct fault_injection_set *fis = &faultInjectionSets[idx];

        for (size_t i = 0; i < fis->finj_set_size; i++)
        {
            struct lreg_node *lrn = &fis->finj_set[i].flti_lrn;

            int rc = lreg_node_remove(
                lrn, LREG_ROOT_ENTRY_PTR(fault_injection_points));

            FATAL_IF(rc, "lreg_node_remove() %s", strerror(-rc));
        }
    }

    faultInjectionSets[idx].finj_set = finj_set;
    faultInjectionSets[idx].finj_set_size = set_size;

    fault_inject_set_lreg_install(&faultInjectionSets[idx]);

    return 0;
}

static init_ctx_t NIOVA_CONSTRUCTOR(FAULT_INJECT_CTOR_PRIORITY)
fault_injection_init(void)
{
    if (faultInjectionInit || !faultInjectionEnabled)
        return;

    faultInjectionInit = true;

    LREG_ROOT_ENTRY_INSTALL(fault_injection_points);

    for (size_t n = 0; n < numFaultInjectionSets; n++)
        fault_inject_set_lreg_install(&faultInjectionSets[n]);
}
