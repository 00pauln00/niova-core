/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include "common.h"
#include "registry.h"
#include "util_thread.h"
#include "niosd_io.h"
#include "log.h"
#include "binary_hist.h"
#include "niosd_io_stats.h"

LREG_ROOT_ENTRY_GENERATE(nioctx_stats_root_entry, LREG_USER_TYPE_NIOSD_IO_CTX);

static int nicsh_params[NICSH_IO_CTX_STATS_MAX][2] =
{
    {NICSH_DEF_IO_SIZE_START_BIT,     NICSH_DEF_IO_SIZE_NBUCKETS},
    {NICSH_DEF_IO_SIZE_START_BIT,     NICSH_DEF_IO_SIZE_NBUCKETS},
    {NICSH_DEF_IO_LAT_START_BIT,      NICSH_DEF_IO_LAT_NBUCKETS},
    {NICSH_DEF_IO_LAT_START_BIT,      NICSH_DEF_IO_LAT_NBUCKETS},
    {NICSH_DEF_IO_TO_CB_START_BIT,    NICSH_DEF_IO_TO_CB_LAT_NBUCKETS},
    {NICSH_DEF_IO_NUM_PDNG_START_BIT, NICSH_DEF_IO_NUM_PDNG_NBUCKETS},
};

enum nioctx_lreg_entry_values
{
    NIOCTX_LREG_CTX_TYPE = 0,
    NIOCTX_LREG_STAT_ENTRIES,
    NIOCTX_LREG_MAX,
};

static util_thread_ctx_reg_t
nioctx_stats_hist_lreg_multi_facet_handler(
    enum lreg_node_cb_ops op,
    const struct niosd_io_ctx_stats *niocs,
    struct lreg_value *lv)
{
    if (!lv ||
        lv->lrv_value_idx_in >= binary_hist_size(&niocs->niocs_bh) ||
        op != LREG_NODE_CB_OP_READ_VAL)
        return;

    snprintf(lv->lrv_key_string, LREG_VALUE_STRING_MAX, "%lld",
             binary_hist_lower_bucket_range(&niocs->niocs_bh,
                                            lv->lrv_value_idx_in));

    LREG_VALUE_TO_OUT_SIGNED_INT(lv) =
        binary_hist_get_cnt(&niocs->niocs_bh, lv->lrv_value_idx_in);
}

static util_thread_ctx_reg_int_t
nioctx_stats_hist_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                          struct lreg_value *lv)
{
    const struct niosd_io_ctx_stats *niocs = lrn->lrn_cb_arg;

    if (lv)
        lv->get.lrv_num_keys_out = binary_hist_size(&niocs->niocs_bh);

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;

        strncpy(LREG_VALUE_TO_OUT_STR(lv),
                niosd_io_ctx_stats_hist_2_name(niocs->niocs_stat_type),
                LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        if (!lv)
            return -EINVAL;

        nioctx_stats_hist_lreg_multi_facet_handler(op, niocs, lv);
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE:
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
        return -ENOENT;
    }

    return 0;
}

static util_thread_ctx_reg_t
nioctx_stats_lreg_multi_facet_handler(enum lreg_node_cb_ops op,
                                      const struct niosd_io_ctx *nioctx,
                                      struct lreg_value *lv)
{
    if (!lv ||
        lv->lrv_value_idx_in >= NIOCTX_LREG_MAX ||
        op != LREG_NODE_CB_OP_READ_VAL)
        return;

    switch (lv->lrv_value_idx_in)
    {
    case NIOCTX_LREG_CTX_TYPE:
        strncpy(lv->lrv_key_string, "ctx-type", LREG_VALUE_STRING_MAX);
        strncpy(LREG_VALUE_TO_OUT_STR(lv),
                niosd_io_ctx_type_to_string(nioctx->nioctx_type),
                LREG_VALUE_STRING_MAX);

        lv->get.lrv_request_type_out = LREG_NODE_TYPE_STRING;
        break;

    case NIOCTX_LREG_STAT_ENTRIES:
        strncpy(lv->lrv_key_string, "nioctx-stats", LREG_VALUE_STRING_MAX);
        lv->get.lrv_request_type_out = LREG_NODE_TYPE_OBJECT;
        break;

    default:
        break;
    }
}

static util_thread_ctx_reg_int_t
nioctx_stats_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                     struct lreg_value *lv)
{
    if (lv)
        lv->get.lrv_num_keys_out = NIOCTX_LREG_MAX;

    const struct niosd_io_ctx *nioctx = lrn->lrn_cb_arg;
    const struct niosd_device *ndev =
        niosd_ctx_to_device((struct niosd_io_ctx *)nioctx);

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;

        snprintf(LREG_VALUE_TO_OUT_STR(lv), LREG_VALUE_STRING_MAX, "%s.%c",
                 ndev->ndev_name,
                 niosd_io_ctx_type_to_char(nioctx->nioctx_type));
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        if (!lv)
            return -EINVAL;

        nioctx_stats_lreg_multi_facet_handler(op, nioctx, lv);
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
        return -ENOENT;
    }

    return 0;
}

void
nioctx_stats_dump(const struct niosd_io_ctx *nioctx)
{
    for (int j = 0; j < NICSH_IO_CTX_STATS_MAX; j++)
    {
        const struct binary_hist *bh =
            niosd_io_ctx_2_stats_bh((struct niosd_io_ctx *)nioctx, j);

        for (int k = 0; k < binary_hist_size(bh); k++)
        {
            SIMPLE_LOG_MSG(LL_NOTIFY,
                           "%d: %lld,%lld: %lld", j,
                           binary_hist_lower_bucket_range(bh, k),
                           binary_hist_upper_bucket_range(bh, k),
                           binary_hist_get_cnt(bh, k));
        }
    }
}

static int
nioctx_stats_init_internal_stats(struct niosd_io_ctx *nioctx)
{
    int rc = 0;

    for (int i = 0; i < NICSH_IO_CTX_STATS_MAX && !rc; i++)
    {
        struct niosd_io_ctx_stats *niocs = &nioctx->nioctx_stats[i];

        // stat type can be easily obtained from the registry callback
        niocs->niocs_stat_type = i;

        rc = binary_hist_init(&niocs->niocs_bh,
                              nicsh_params[i][0], nicsh_params[i][1]);

        lreg_node_init(&niocs->niocs_lrn, LREG_NODE_TYPE_OBJECT,
                       LREG_USER_TYPE_NIOSD_IO_CTX_STATS,
                       nioctx_stats_hist_lreg_cb, (void *)niocs, false);

        rc = lreg_node_install_prepare(&niocs->niocs_lrn,
                                       &nioctx->nioctx_lreg_node);

        FATAL_IF(rc, "lreg_node_install_prepare(): %s", strerror(-rc));
    }

    return rc;
}

void
nioctx_stats_init(struct niosd_io_ctx *nioctx)
{
    lreg_node_init(&nioctx->nioctx_lreg_node, LREG_NODE_TYPE_OBJECT,
                   LREG_USER_TYPE_NIOSD_IO_CTX, nioctx_stats_lreg_cb, nioctx,
                   false);

    int rc =
        lreg_node_install_prepare(&nioctx->nioctx_lreg_node,
                                  LREG_ROOT_ENTRY_PTR(nioctx_stats_root_entry));

    FATAL_IF(rc, "lreg_node_install_prepare(): %s", strerror(-rc));

    rc = nioctx_stats_init_internal_stats(nioctx);
    FATAL_IF(rc, "nioctx_stats_init_ctx_stats(): %s", strerror(-rc));
}

void
nioctx_stats_ingest_from_niorq(struct niosd_io_request *niorq)
{
    if (!niorq ||
        (niorq->niorq_type != NIOSD_REQ_TYPE_PREAD &&
         niorq->niorq_type != NIOSD_REQ_TYPE_PWRITE))
        return;

    struct niosd_io_ctx *nioctx = niorq->niorq_ctx;

    /* I/O Size
     */
    struct binary_hist *bh =
        niosd_io_ctx_2_stats_bh(nioctx,
                                (niorq->niorq_type == NIOSD_REQ_TYPE_PREAD ?
                                 NICSH_RD_SIZE_IN_BYTES :
                                 NICSH_WR_SIZE_IN_BYTES));

    binary_hist_incorporate_val(bh, niosd_io_request_nsectors_to_bytes(niorq));

    /* Latency related stats
     */
    long long lat;

    bh = niosd_io_ctx_2_stats_bh(nioctx,
                                 (niorq->niorq_type == NIOSD_REQ_TYPE_PREAD ?
                                  NICSH_RD_LATENCY_USEC :
                                  NICSH_WR_LATENCY_USEC));
    int rc =
        niosd_io_request_latency_stages_usec(niorq,
                                             NIOSD_IO_REQ_TIMER_SUBMITTED,
                                             NIOSD_IO_REQ_TIMER_EVENT_REAPED,
                                             &lat);
    if (!rc)
        binary_hist_incorporate_val(bh, lat);

    bh = niosd_io_ctx_2_stats_bh(nioctx, NICSH_IO_TO_CB_TIME_USEC);

    rc =
        niosd_io_request_latency_stages_usec(niorq,
                                             NIOSD_IO_REQ_TIMER_EVENT_REAPED,
                                             NIOSD_IO_REQ_TIMER_CB_EXEC,
                                             &lat);
    if (!rc)
        binary_hist_incorporate_val(bh, lat);

//    DBG_NIOSD_REQ(LL_WARN, niorq, "lat=%lld rc=%d", lat, rc);

    /* Pending I/O Depth
     */
    bh = niosd_io_ctx_2_stats_bh(nioctx, NICSH_IO_NUM_PENDING);
    binary_hist_incorporate_val(bh, niosd_ctx_pending_io_ops(nioctx));
}

init_ctx_t
nioctx_stats_subsys_init(void)
{
    SIMPLE_FUNC_ENTRY(LL_DEBUG);

    LREG_ROOT_ENTRY_INSTALL(nioctx_stats_root_entry);
}

destroy_ctx_t
nioctx_stats_subsys_destroy(void)
{
    SIMPLE_FUNC_ENTRY(LL_DEBUG);
}
