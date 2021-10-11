/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <fcntl.h>
#include <linux/limits.h>
#include <regex.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include "alloc.h"
#include "crc32.h"
#include "ctl_interface.h"
#include "ctl_svc.h"
#include "ctor.h"
#include "epoll_mgr.h"
#include "init.h"
#include "io.h"
#include "log.h"
#include "raft.h"
#include "random.h"
#include "regex_defines.h"
#include "udp.h"
#include "util_thread.h"

#define DEFAULT_BULK_CREDITS 32
#define DEFAULT_INCOMING_CREDITS 32

enum raft_net_lreg_values
{
    RAFT_NET_LREG_IGNORE_TIMER_EVENTS,
    RAFT_NET_LREG_ELECTION_TIMEOUT_MS,// uint32
    RAFT_NET_LREG_HEARTBEAT_FREQ,     // uint32
    RAFT_NET_LREG_MAX_SCAN_ENTRIES,   // int64
    RAFT_NET_LREG_LOG_REAP_FACTOR,    // uint32
    RAFT_NET_LREG_NUM_CHECKPOINTS,
    RAFT_NET_LREG_AUTO_CHECKPOINT,
    RAFT_NET_LREG__MAX,
    RAFT_NET_LREG__CLIENT_MAX = RAFT_NET_LREG_IGNORE_TIMER_EVENTS + 1,
};

enum raft_net_recovery_lreg_values
{
    RAFT_NET_RECOVERY_LREG_PEER_UUID,
    RAFT_NET_RECOVERY_LREG_DB_UUID,
    RAFT_NET_RECOVERY_LREG_CHKPT_IDX,
    RAFT_NET_RECOVERY_LREG_CHKPT_SIZE,
    RAFT_NET_RECOVERY_LREG_REMAINING,
    RAFT_NET_RECOVERY_LREG_COMPLETED,
    RAFT_NET_RECOVERY_LREG_RATE,
    RAFT_NET_RECOVERY_LREG_INCOMPLETE,
    RAFT_NET_RECOVERY_LREG_START_TIME,
    RAFT_NET_RECOVERY_LREG__MAX,
    RAFT_NET_RECOVERY_LREG__NONE = 0,
};

struct raft_instance raftInstance = {
//    .ri_store_type = RAFT_INSTANCE_STORE_ROCKSDB,
    .ri_store_type = RAFT_INSTANCE_STORE_POSIX_FLAT_FILE,
};

static regex_t raftNetRncuiRegex;

REGISTRY_ENTRY_FILE_GENERATE;

static util_thread_ctx_reg_int_t
raft_net_lreg_multi_facet_cb(enum lreg_node_cb_ops, struct lreg_value *,
                             void *);

static util_thread_ctx_reg_int_t
raft_net_recovery_lreg_multi_facet_cb(enum lreg_node_cb_ops,
                                      struct lreg_value *, void *);

struct raft_instance *
raft_net_get_instance(void)
{
    // Xxx this needs to become more flexible so that more than 1 instance
    //      may be serviced by a single process
    return &raftInstance;
}

static unsigned int
raft_net_lreg_num_keys(void)
{
    return raft_instance_is_client(raft_net_get_instance()) ?
        RAFT_NET_LREG__CLIENT_MAX : RAFT_NET_LREG__MAX;
}

static unsigned int
raft_net_lreg_recovery_num_keys(void)
{
    const struct raft_instance *ri = raft_net_get_instance();

    return (!raft_instance_is_client(ri) &&
            (ri->ri_needs_bulk_recovery || ri->ri_successful_recovery)) ?
        RAFT_NET_RECOVERY_LREG__MAX : RAFT_NET_RECOVERY_LREG__NONE;
}

LREG_ROOT_ENTRY_GENERATE_OBJECT(raft_net_info, LREG_USER_TYPE_RAFT_NET,
                                raft_net_lreg_num_keys(),
                                raft_net_lreg_multi_facet_cb, NULL,
                                LREG_INIT_OPT_NONE);

LREG_ROOT_ENTRY_GENERATE_OBJECT(raft_net_bulk_recovery_info,
                                LREG_USER_TYPE_RAFT_RECOVERY_NET,
                                raft_net_lreg_recovery_num_keys(),
                                raft_net_recovery_lreg_multi_facet_cb, NULL,
                                LREG_INIT_OPT_NONE);

static unsigned int
raft_net_calc_max_heartbeat_freq(const struct raft_instance *ri)
{
    return ri->ri_election_timeout_max_ms / RAFT_HEARTBEAT__MIN_TIME_MS;
}

static void
raft_net_set_heartbeat_freq(struct raft_instance *ri, unsigned int new_hb_freq)
{
    if (!ri || new_hb_freq < RAFT_HEARTBEAT__MIN_FREQ)
        return;

    const unsigned int election_timeout = ri->ri_election_timeout_max_ms;
    const unsigned int max_hb_freq = raft_net_calc_max_heartbeat_freq(ri);

    if (new_hb_freq > max_hb_freq)
    {
        SIMPLE_LOG_MSG(
            LL_NOTIFY,
            "raft heartbeat freq (%u) is too high for election timeout (%u), defaulting to %u",
            new_hb_freq, election_timeout, RAFT_HEARTBEAT__MIN_TIME_MS);

        new_hb_freq = max_hb_freq;
    }

    if (new_hb_freq != ri->ri_heartbeat_freq_per_election_min)
    {
        ri->ri_heartbeat_freq_per_election_min = new_hb_freq;
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft heartbeat freq = %u", new_hb_freq);
    }
}

static int
raft_net_lreg_set_heartbeat_freq(struct raft_instance *ri,
                                 const struct lreg_value *lv)
{
    if (!ri || !lv || LREG_VALUE_TO_REQ_TYPE_IN(lv) != LREG_VAL_TYPE_STRING)
        return -EINVAL;

    unsigned int hb_freq = RAFT_HEARTBEAT_FREQ_PER_ELECTION;

    if (strncmp(LREG_VALUE_TO_IN_STR(lv), "default", 7))
    {
        int rc = niova_string_to_unsigned_int(LREG_VALUE_TO_IN_STR(lv),
                                              &hb_freq);
        if (rc)
            return rc;
    }

    if (hb_freq == ri->ri_heartbeat_freq_per_election_min)
        return 0; // noop return

    if (hb_freq < RAFT_HEARTBEAT__MIN_FREQ)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY,
                       "raft heartbeat freq (%u) is out of range (min=%u)",
                       hb_freq, RAFT_HEARTBEAT__MIN_FREQ);
        return -ERANGE;
    }

    raft_net_set_heartbeat_freq(ri, hb_freq);

    return 0;
}

static int
raft_net_lreg_set_election_timeout(struct raft_instance *ri,
                                   const struct lreg_value *lv)
{
    if (!ri || !lv || LREG_VALUE_TO_REQ_TYPE_IN(lv) != LREG_VAL_TYPE_STRING)
        return -EINVAL;

    unsigned long long election_timeout = RAFT_ELECTION_UPPER_TIME_MS;

    if (strncmp(LREG_VALUE_TO_IN_STR(lv), "default", 7))
    {
        int rc = niova_string_to_unsigned_long_long(LREG_VALUE_TO_IN_STR(lv),
                                                    &election_timeout);
        if (rc)
            return rc;
    }

    if (election_timeout > RAFT_ELECTION__MAX_TIME_MS ||
        election_timeout < RAFT_ELECTION__MIN_TIME_MS)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft election timeout (%llu) out of range",
                       election_timeout);
        return -ERANGE;
    }

    if (election_timeout != ri->ri_election_timeout_max_ms)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft election timeout %u -> %llu",
                       ri->ri_election_timeout_max_ms, election_timeout);

        ri->ri_election_timeout_max_ms = election_timeout;
        // Adjust the hb freq as needed
        raft_net_set_heartbeat_freq(ri,
                                    ri->ri_heartbeat_freq_per_election_min);
    }

    return 0;
}

void
raft_net_set_max_scan_entries(struct raft_instance *ri,
                              ssize_t max_scan_entries)
{
    NIOVA_ASSERT(ri);

    if (max_scan_entries < 0)
        ri->ri_max_scan_entries = (ssize_t)-1;

    else
        ri->ri_max_scan_entries =
            MAX(max_scan_entries,
                RAFT_INSTANCE_PERSISTENT_APP_MIN_SCAN_ENTRIES);

    SIMPLE_LOG_MSG(LL_WARN, "max_scan_entries=%zd", ri->ri_max_scan_entries);
}

static int
raft_net_lreg_set_max_scan_entries(struct raft_instance *ri,
                                   const struct lreg_value *lv)
{
    if (!ri || !lv || LREG_VALUE_TO_REQ_TYPE_IN(lv) != LREG_VAL_TYPE_STRING)
        return -EINVAL;

    ssize_t max_scan_entries = RAFT_INSTANCE_PERSISTENT_APP_SCAN_ENTRIES;

    if (strncmp(LREG_VALUE_TO_IN_STR(lv), "default", 7))
    {
        long long tmp = 0;
        int rc = niova_string_to_long_long(LREG_VALUE_TO_IN_STR(lv), &tmp);
        if (rc)
            return rc;

        max_scan_entries = tmp;
    }

    raft_net_set_max_scan_entries(ri, max_scan_entries);
    return 0;
}

void
raft_net_set_log_reap_factor(struct raft_instance *ri, size_t log_reap_factor)
{
    NIOVA_ASSERT(ri);

    ri->ri_log_reap_factor = MIN(log_reap_factor,
                                 RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_MAX);

    SIMPLE_LOG_MSG(LL_WARN, "log_reap_factor=%zu", ri->ri_log_reap_factor);
}

static int
raft_net_lreg_set_log_reap_factor(struct raft_instance *ri,
                                  const struct lreg_value *lv)
{
    if (!ri || !lv || LREG_VALUE_TO_REQ_TYPE_IN(lv) != LREG_VAL_TYPE_STRING)
        return -EINVAL;

    size_t lrf = RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR;

    if (strncmp(LREG_VALUE_TO_IN_STR(lv), "default", 7))
    {
        unsigned long long tmp = 0;
        int rc =
            niova_string_to_unsigned_long_long(LREG_VALUE_TO_IN_STR(lv), &tmp);

        if (rc)
            return rc;

        lrf = tmp;
    }

    raft_net_set_log_reap_factor(ri, lrf);

    return 0;
}

void
raft_net_set_num_checkpoints(struct raft_instance *ri, size_t num_ckpts)
{
    NIOVA_ASSERT(ri);

    num_ckpts = MAX(RAFT_INSTANCE_PERSISTENT_APP_CHKPT_MIN, num_ckpts);

    ri->ri_num_checkpoints = MIN(num_ckpts,
                                 RAFT_INSTANCE_PERSISTENT_APP_CHKPT_MAX);

    SIMPLE_LOG_MSG(LL_WARN, "num_checkpoints=%zu", ri->ri_num_checkpoints);
}

static int
raft_net_lreg_set_num_checkpoints(struct raft_instance *ri,
                                  const struct lreg_value *lv)
{
    if (!ri || !lv || LREG_VALUE_TO_REQ_TYPE_IN(lv) != LREG_VAL_TYPE_STRING)
        return -EINVAL;

    size_t nchkpt = RAFT_INSTANCE_PERSISTENT_APP_CHKPT;

    if (strncmp(LREG_VALUE_TO_IN_STR(lv), "default", 7))
    {
        unsigned long long tmp = 0;
        int rc =
            niova_string_to_unsigned_long_long(LREG_VALUE_TO_IN_STR(lv), &tmp);

        if (rc)
            return rc;

        nchkpt = tmp;
    }

    raft_net_set_num_checkpoints(ri, nchkpt);

    return 0;
}

static util_thread_ctx_reg_int_t
raft_net_recovery_lreg_multi_facet_cb(enum lreg_node_cb_ops op,
                                      struct lreg_value *lv, void *arg)
{
    if (arg)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= RAFT_NET_RECOVERY_LREG__MAX)
        return -ERANGE;

    struct raft_instance *ri = raft_net_get_instance();
    NIOVA_ASSERT(ri);

    struct raft_recovery_handle *rrh = &ri->ri_recovery_handle;
    int rc = 0;

    switch (op)
    {
    case LREG_NODE_CB_OP_READ_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case RAFT_NET_RECOVERY_LREG_PEER_UUID:
            lreg_value_fill_string_uuid(lv, "src-uuid", rrh->rrh_peer_uuid);
            break;
        case RAFT_NET_RECOVERY_LREG_DB_UUID:
            lreg_value_fill_string_uuid(lv, "db-uuid", rrh->rrh_peer_db_uuid);
            break;
        case RAFT_NET_RECOVERY_LREG_CHKPT_IDX:
            lreg_value_fill_signed(lv, "chkpt-idx", rrh->rrh_peer_chkpt_idx);
            break;
        case RAFT_NET_RECOVERY_LREG_CHKPT_SIZE:
            lreg_value_fill_signed(lv, "chkpt-size", rrh->rrh_chkpt_size);
            break;
        case RAFT_NET_RECOVERY_LREG_REMAINING:
            lreg_value_fill_signed(lv, "total-bytes-to-xfer",
                                   rrh->rrh_remaining);
            break;
        case RAFT_NET_RECOVERY_LREG_COMPLETED:
            lreg_value_fill_signed(lv, "xfer-completed", rrh->rrh_completed);
            break;
        case RAFT_NET_RECOVERY_LREG_INCOMPLETE:
            lreg_value_fill_bool(lv, "resume-incomplete",
                                 ri->ri_incomplete_recovery);
            break;
        case RAFT_NET_RECOVERY_LREG_START_TIME:
            lreg_value_fill_string_time(lv, "start-time",
                                        rrh->rrh_start.tv_sec);
            break;
        case RAFT_NET_RECOVERY_LREG_RATE:
            lreg_value_fill_string(lv, "rate", rrh->rrh_rate_bytes_per_sec);
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

static util_thread_ctx_reg_int_t
raft_net_lreg_multi_facet_cb(enum lreg_node_cb_ops op, struct lreg_value *lv,
                             void *arg)
{
    if (arg)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= RAFT_NET_LREG__MAX)
        return -ERANGE;

    struct raft_instance *ri = raft_net_get_instance();
    NIOVA_ASSERT(ri);

    int rc = 0;
    bool tmp_bool = false;

    switch (op)
    {
    case LREG_NODE_CB_OP_READ_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case RAFT_NET_LREG_IGNORE_TIMER_EVENTS:
            lreg_value_fill_bool(lv, "ignore_timer_events",
                                 ri->ri_ignore_timerfd ? true : false);
            break;
        case RAFT_NET_LREG_ELECTION_TIMEOUT_MS:
            lreg_value_fill_unsigned(lv, "election-timeout-ms",
                                     ri->ri_election_timeout_max_ms);
            break;
        case RAFT_NET_LREG_HEARTBEAT_FREQ:
            lreg_value_fill_unsigned(lv, "heartbeat-freq-per-election-timeout",
                                     ri->ri_heartbeat_freq_per_election_min);
            break;
        case RAFT_NET_LREG_MAX_SCAN_ENTRIES:
            lreg_value_fill_signed(lv, "max-scan-entries",
                                   ri->ri_max_scan_entries);
            break;
        case RAFT_NET_LREG_LOG_REAP_FACTOR:
            lreg_value_fill_unsigned(lv, "log-reap-factor",
                                     ri->ri_log_reap_factor);
            break;
        case RAFT_NET_LREG_NUM_CHECKPOINTS:
            lreg_value_fill_unsigned(lv, "num-checkpoints",
                                     ri->ri_num_checkpoints);
            break;
        case RAFT_NET_LREG_AUTO_CHECKPOINT:
            lreg_value_fill_bool(lv, "auto-checkpoint-enabled",
                                 ri->ri_auto_checkpoints_enabled);
            break;
        default:
            rc = -ENOENT;
            break;
        }
        break;
    case LREG_NODE_CB_OP_WRITE_VAL:
        // Note that all WRITE_VAL inputs are strings.
        if (lv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        switch (lv->lrv_value_idx_in)
        {
        case RAFT_NET_LREG_IGNORE_TIMER_EVENTS:
            rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lv), &tmp_bool);
            if (rc)
                return rc;

            ri->ri_ignore_timerfd = tmp_bool;
            break;
        case RAFT_NET_LREG_AUTO_CHECKPOINT:
            rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lv), &tmp_bool);
            if (rc)
                return rc;
            if (ri->ri_auto_checkpoints_enabled != tmp_bool)
                ri->ri_auto_checkpoints_enabled = tmp_bool;
            break;
        case RAFT_NET_LREG_ELECTION_TIMEOUT_MS:
            rc = raft_net_lreg_set_election_timeout(ri, lv);
            break;
        case RAFT_NET_LREG_HEARTBEAT_FREQ:
            rc = raft_net_lreg_set_heartbeat_freq(ri, lv);
            break;
        case RAFT_NET_LREG_MAX_SCAN_ENTRIES:
            rc = raft_net_lreg_set_max_scan_entries(ri, lv);
            break;
        case RAFT_NET_LREG_LOG_REAP_FACTOR:
            rc = raft_net_lreg_set_log_reap_factor(ri, lv);
            break;
        case RAFT_NET_LREG_NUM_CHECKPOINTS:
            rc = raft_net_lreg_set_num_checkpoints(ri, lv);
            break;
        default:
            rc = -EPERM;
            break;
        }
        break;
    default:
        rc = -EOPNOTSUPP;
        break;
    }

    return rc;
}

static bool
raft_net_tcp_disabled(void)
{
    const struct niova_env_var *ev = env_get(NIOVA_ENV_VAR_tcp_enable);
    return !(ev && ev->nev_present);
}

static int
raft_net_tcp_sockets_close(struct raft_instance *ri)
{
    int rc = tcp_mgr_sockets_close(&ri->ri_peer_tcp_mgr);
    int rc2 = tcp_mgr_sockets_close(&ri->ri_client_tcp_mgr);

    return rc ? rc : rc2;
}

static int
raft_net_udp_sockets_close(struct raft_instance *ri)
{
    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX; i++)
    {
        int tmp_rc = udp_socket_close(&ri->ri_ush[i]);
        if (tmp_rc && !rc) // store the first error found.
            rc = tmp_rc;
    }

    return rc;
}

static int
raft_net_sockets_close(struct raft_instance *ri)
{
    int rc, rc2;

    rc = raft_net_udp_sockets_close(ri);
    if (raft_net_tcp_disabled())
        return rc;

    rc2 = raft_net_tcp_sockets_close(ri);

    return rc ? rc : rc2;
}

static int
raft_net_tcp_sockets_bind(struct raft_instance *ri)
{
    int rc = tcp_mgr_sockets_bind(&ri->ri_peer_tcp_mgr);

    return rc ? rc : tcp_mgr_sockets_bind(&ri->ri_client_tcp_mgr);
}

static int
raft_net_udp_sockets_bind(struct raft_instance *ri)
{
    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX && !rc; i++)
    {
        if (raft_instance_is_client(ri) && i == RAFT_UDP_LISTEN_SERVER)
            continue;

        rc = udp_socket_bind(&ri->ri_ush[i]);
    }

    if (rc)
        raft_net_udp_sockets_close(ri);

    return rc;
}

int
raft_net_sockets_bind(struct raft_instance *ri)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    int rc;

    rc = raft_net_udp_sockets_bind(ri);
    if (rc || raft_net_tcp_disabled())
        return rc;

    rc = raft_net_tcp_sockets_bind(ri);

    return rc;
}

static int
raft_net_tcp_sockets_setup(struct raft_instance *ri)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!ri)
        return -EINVAL;

    const char *ipaddr = ctl_svc_node_peer_2_ipaddr(ri->ri_csn_this_peer);
    int peer_port = ctl_svc_node_peer_2_port(ri->ri_csn_this_peer);
    int client_port = ctl_svc_node_peer_2_client_port(ri->ri_csn_this_peer);

    int rc = tcp_mgr_sockets_setup(&ri->ri_peer_tcp_mgr, ipaddr, peer_port);
    return rc ? rc
        : tcp_mgr_sockets_setup(&ri->ri_client_tcp_mgr, ipaddr, client_port);
}

static int
raft_net_udp_sockets_setup(struct raft_instance *ri)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!ri)
        return -EINVAL;

    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX; i++)
    {
        strncpy(ri->ri_ush[i].ush_ipaddr,
                ctl_svc_node_peer_2_ipaddr(ri->ri_csn_this_peer), IPV4_STRLEN);

        if (i == RAFT_UDP_LISTEN_SERVER) // server <-> server comms port
        {
            if (raft_instance_is_client(ri))
                continue; // no server listen port in client mode

            ri->ri_ush[i].ush_port =
                ctl_svc_node_peer_2_port(ri->ri_csn_this_peer);
        }
        else if (i == RAFT_UDP_LISTEN_CLIENT) // client <-> server port
        {
            ri->ri_ush[i].ush_port =
                ctl_svc_node_peer_2_client_port(ri->ri_csn_this_peer);
        }
        else
        {
            rc = -ESOCKTNOSUPPORT;
            break;
        }

        if (!ri->ri_ush[i].ush_port)
        {
            rc = -ENOENT;
            break;
        }

        rc = udp_socket_setup(&ri->ri_ush[i]);
        if (rc)
            break;
    }

    if (rc)
        raft_net_udp_sockets_close(ri);

    return rc;
}

int
raft_net_sockets_setup(struct raft_instance *ri)
{
    int rc;

    rc = raft_net_udp_sockets_setup(ri);
    if (rc || raft_net_tcp_disabled())
        return rc;

    rc = raft_net_tcp_sockets_setup(ri);

    return rc;
}

static int
raft_net_timerfd_create(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    ri->ri_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (ri->ri_timer_fd < 0)
        return -errno;

    return 0;
}

static int
raft_net_timerfd_close(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    if (ri->ri_timer_fd >= 0)
    {
        int tmp_fd = ri->ri_timer_fd;
        ri->ri_timer_fd = -1;

        return close(tmp_fd);
    }

    return 0;
}

static int
raft_net_epoll_cleanup(struct raft_instance *ri)
{
    for (enum raft_epoll_handles i = 0; i < RAFT_EPOLL_HANDLES_MAX; i++)
    {
        NIOVA_ASSERT(
            epoll_handle_releases_in_current_thread(&ri->ri_epoll_mgr,
                                                    &ri->ri_epoll_handles[i]));

        epoll_handle_del(&ri->ri_epoll_mgr, &ri->ri_epoll_handles[i]);
    }

    return epoll_mgr_close(&ri->ri_epoll_mgr);
}

/**
 *  raft_net_udp_cb - forward declaration for the generic udp recv handler.
 */
static raft_net_cb_ctx_t
raft_net_udp_cb(const struct epoll_handle *, uint32_t);

static int
raft_net_epoll_handle_add(struct raft_instance *ri, int fd, epoll_mgr_cb_t cb)
{
    if (!ri || fd < 0 || !cb)
        return -EINVAL;

    struct epoll_handle *eph = NULL;
    for (int i = 0; i < RAFT_EPOLL_HANDLES_MAX; i++)
    {
        if (!epoll_handle_is_installed(&ri->ri_epoll_handles[i]))
        {
            eph = &ri->ri_epoll_handles[i];
            break;
        }
    }

    if (!eph)
        return -ENOSPC;

    int rc = epoll_handle_init(eph, fd, EPOLLIN, cb, ri, NULL);
    if (rc)
        return rc;

    rc = epoll_handle_add(&ri->ri_epoll_mgr, eph);
    if (rc)
        NIOVA_ASSERT(!epoll_handle_is_installed(eph));

    return rc;
}

static void
raft_net_epoll_ensure_handles_are_removed(const struct raft_instance *ri)
{
    for (int i = 0; i < RAFT_EPOLL_HANDLES_MAX; i++)
        NIOVA_ASSERT(!epoll_handle_is_installed(&ri->ri_epoll_handles[i]));
}

static int
raft_net_epoll_handle_remove_by_fd(struct raft_instance *ri, int fd)
{
    if (!ri || fd < 0)
	return -EINVAL;

    if (!epoll_mgr_is_ready(&ri->ri_epoll_mgr))
    {
        raft_net_epoll_ensure_handles_are_removed(ri);
        return -ESHUTDOWN;
    }

    int rc = 0;
    int removed_cnt = 0;

    for (int i = 0; i < RAFT_EPOLL_HANDLES_MAX; i++)
    {
        struct epoll_handle *eph = &ri->ri_epoll_handles[i];
        if (epoll_handle_is_installed(eph) && eph->eph_fd == fd)
        {
            NIOVA_ASSERT(
                epoll_handle_releases_in_current_thread(&ri->ri_epoll_mgr,
                                                        eph));

            int tmp_rc = epoll_handle_del(&ri->ri_epoll_mgr, eph);
            if (tmp_rc)
            {
                SIMPLE_LOG_MSG(LL_WARN, "epoll_handle_del(%p): %s ",
                               eph, strerror(-tmp_rc));
                if (!rc)
                    rc = tmp_rc;
            }
            else
            {
                NIOVA_ASSERT(!epoll_handle_is_installed(eph));
                removed_cnt++;
            }
        }
    }

    return (rc || removed_cnt) ? rc : -ENOENT;
}

static int
raft_epoll_setup_udp(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX && !rc; i++)
    {
        if (raft_instance_is_client(ri) && i == RAFT_UDP_LISTEN_SERVER)
            continue;

        rc = raft_net_epoll_handle_add(ri, ri->ri_ush[i].ush_socket,
                                       raft_net_udp_cb);
    }

    return rc;
}

static int
raft_epoll_setup_net(struct raft_instance *ri)
{
    int rc;

    rc = raft_epoll_setup_udp(ri);
    if (rc || raft_net_tcp_disabled())
        return rc;

    rc = tcp_mgr_epoll_setup(&ri->ri_peer_tcp_mgr, &ri->ri_epoll_mgr);
    return rc ? rc
        : tcp_mgr_epoll_setup(&ri->ri_client_tcp_mgr, &ri->ri_epoll_mgr);
}

static bool
raft_net_evp_type_valid(const struct raft_instance *ri,
                        enum raft_event_pipe_types type)
{
    return (!ri || type == RAFT_EVP__NONE || type == RAFT_EVP__ANY   ||
            (raft_instance_is_client(ri) && type != RAFT_EVP_CLIENT) ||
            (!raft_instance_is_client(ri) && type == RAFT_EVP_CLIENT))
        ? false
        : true;
}

struct ev_pipe *
raft_net_evp_get(struct raft_instance *ri, enum raft_event_pipe_types type)
{
    for (int i = 0; i < RAFT_EVP_HANDLES_MAX; i++)
    {
        if (ri->ri_evps[i].revp_type == type)
            return &ri->ri_evps[i].revp_evp;
    }

    return NULL;
}

int
raft_net_evp_notify(struct raft_instance *ri, enum raft_event_pipe_types type)
{
    if (!raft_net_evp_type_valid(ri, type))
        return -EINVAL;

    struct ev_pipe *evp = raft_net_evp_get(ri, type);
    if (evp)
    {
        ev_pipe_notify(evp);
        return 0;
    }

    return -ENOENT;
}

int
raft_net_evp_add(struct raft_instance *ri, epoll_mgr_cb_t cb,
                 enum raft_event_pipe_types type)
{
    if (!ri || !cb || !raft_net_evp_type_valid(ri, type))
        return -EINVAL;

    int idx = -1;
    for (int i = 0; i < RAFT_EVP_HANDLES_MAX; i++)
    {
        if (ri->ri_evps[i].revp_type == RAFT_EVP__NONE)
            idx = i; // Don't break, continue to end looking for dup

        else if (ri->ri_evps[i].revp_type == type)
            return -EALREADY;
    }

    if (idx == -1)
        return -ENOSPC;

    struct raft_evp *revp = &ri->ri_evps[idx];

    int rc = ev_pipe_setup(&revp->revp_evp);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "ev_pipe_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = raft_net_epoll_handle_add(ri, evp_read_fd_get(&revp->revp_evp), cb);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_epoll_handle_add(): %s",
                       strerror(-rc));

        int cleanup_rc = ev_pipe_cleanup(&revp->revp_evp);
        if (cleanup_rc)
            SIMPLE_LOG_MSG(LL_ERROR, "ev_pipe_cleanup(): %s",
                           strerror(-cleanup_rc));

        return rc;
    }

    revp->revp_type = type;
    revp->revp_installed_on_epm = 1;

    return 0;
}

int
raft_net_evp_remove(struct raft_instance *ri, enum raft_event_pipe_types type)
{
    if (!ri || !raft_net_evp_type_valid(ri, type))
        return -EINVAL;

    int idx = -1;
    for (int i = 0; i < RAFT_EVP_HANDLES_MAX; i++)
    {
        if (ri->ri_evps[i].revp_type == type)
            idx = i; // Don't break, continue to end looking for dup

        else
            NIOVA_ASSERT(ri->ri_evps[i].revp_type != type);
    }

    if (idx == -1)
        return -ENOENT;

    struct raft_evp *revp = &ri->ri_evps[idx];

    // For now since all evp are placed onto an epm.
    NIOVA_ASSERT(revp->revp_installed_on_epm);

    int rc = 0;
    int epoll_remove_rc =
        raft_net_epoll_handle_remove_by_fd(ri,
                                           evp_read_fd_get(&revp->revp_evp));

    /* raft_net_epoll_handle_remove_by_fd() checks for cleanup of all epoll
     * handles in the case where the epm has been already shutdown.
     */
    if (epoll_remove_rc && epoll_remove_rc != -ESHUTDOWN)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_epoll_handle_remove_by_fd(%d): %s",
                       type, strerror(-epoll_remove_rc));
        if (!rc)
            rc = epoll_remove_rc;
    }

    int ev_pipe_cleanup_rc = ev_pipe_cleanup(&revp->revp_evp);
    if (ev_pipe_cleanup_rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "ev_pipe_cleanup_rc(%d): %s",
                       type, strerror(-ev_pipe_cleanup_rc));
        if (!rc)
            rc = ev_pipe_cleanup_rc;
    }

    revp->revp_type = RAFT_EVP__NONE;
    revp->revp_installed_on_epm = 0;

    return rc;
}

raft_net_timerfd_cb_ctx_t
raft_net_timerfd_cb(const struct epoll_handle *, uint32_t);

static int
raft_net_epoll_setup_timerfd(struct raft_instance *ri)
{
    if (!ri ||
        (!raft_instance_is_client(ri) && !ri->ri_timer_fd_cb))
        return -EINVAL; // Servers must have specified ri_timer_fd_cb

    else if (!ri->ri_timer_fd_cb)
        return 0;

    return raft_net_epoll_handle_add(ri, ri->ri_timer_fd, raft_net_timerfd_cb);
}

static int
raft_net_epoll_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = epoll_mgr_setup(&ri->ri_epoll_mgr);
    if (rc)
        return rc;

    /* Add the timerfd to the epoll_mgr.
     */
    rc = raft_net_epoll_setup_timerfd(ri);

    rc = raft_epoll_setup_net(ri);

    if (rc)
        raft_net_epoll_cleanup(ri);

    return rc;
}

static void
raft_net_conf_destroy(struct raft_instance *ri)
{
    if (ri->ri_csn_raft)
        ctl_svc_node_put(ri->ri_csn_raft);

    if (ri->ri_csn_this_peer)
        ctl_svc_node_put(ri->ri_csn_this_peer);

    for (int i = 0; i < CTL_SVC_MAX_RAFT_PEERS; i++)
        if (ri->ri_csn_raft_peers[i])
            ctl_svc_node_put(ri->ri_csn_raft_peers[i]);
}

/**
 * raft_server_instance_conf_init - Initialize this raft instance's config
 *    based on the 2 UUIDs passed in at startup time.  These UUIDs are for
 *    the Raft instance itself (and the peers involved) and the peer UUID for
 *    this instance.  The role of this function is to obtain the ctl_svc_node
 *    objects which pertain to these UUIDs so that basic config information
 *    can be obtained, such as: IP addresses, port numbers, and the raft log
 *    pathname.
 */
static int
raft_net_conf_init(struct raft_instance *ri)
{
    uuid_t tmp, tmp1;
    /* Check the ri for the needed the UUID strings.
     */
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str ||
        uuid_parse(ri->ri_this_peer_uuid_str, tmp) ||
        uuid_parse(ri->ri_raft_uuid_str, tmp1) ||
        !uuid_compare(tmp, tmp1))
        return -EINVAL;

    for (int i = RAFT_UDP_LISTEN_MIN; i < RAFT_UDP_LISTEN_MAX; i++)
        udp_socket_handle_init(&ri->ri_ush[i]);

    /* (re)initialize the ctl-svc node pointers.
     */
    ri->ri_csn_raft = NULL;
    ri->ri_csn_this_peer = NULL;
    for (int i = 0; i < CTL_SVC_MAX_RAFT_PEERS; i++)
        ri->ri_csn_raft_peers[i] = NULL;

    /* Lookup 'this' node's ctl-svc object.
     */
    int rc = ctl_svc_node_lookup_by_string(ri->ri_this_peer_uuid_str,
                                           &ri->ri_csn_this_peer);
    if (rc)
    {
        LOG_MSG(LL_ERROR,
                "ctl_svc_node_lookup() failed to find self UUID=%s\n"
                "Please check the local-control-service directory: %s",
                ri->ri_this_peer_uuid_str, ctl_svc_get_local_dir());

        goto cleanup;
    }

    /* Lookup the raft ctl-svc object.
     */
    rc = ctl_svc_node_lookup_by_string(ri->ri_raft_uuid_str, &ri->ri_csn_raft);
    if (rc)
    {
        LOG_MSG(LL_ERROR,
                "ctl_svc_node_lookup() failed to find raft UUID=%s\n"
                "Please check the local-control-service directory: %s",
                ri->ri_raft_uuid_str, ctl_svc_get_local_dir());

        goto cleanup;
    }

    DBG_CTL_SVC_NODE(LL_NOTIFY, ri->ri_csn_this_peer, "self");
    DBG_CTL_SVC_NODE(LL_NOTIFY, ri->ri_csn_raft, "raft");

    const struct ctl_svc_node_raft *csn_raft =
        ctl_svc_node_raft_2_raft(ri->ri_csn_raft);

    if (!csn_raft)
    {
        rc = -EINVAL;
        goto cleanup;
    }
    else if (csn_raft->csnr_num_members > CTL_SVC_MAX_RAFT_PEERS)
    {
        rc = -E2BIG;
        goto cleanup;
    }

    bool this_peer_found_in_raft_node = false;
    for (raft_peer_t i = 0; i < csn_raft->csnr_num_members; i++)
    {
        rc = ctl_svc_node_lookup(csn_raft->csnr_members[i].csrm_peer,
                                 &ri->ri_csn_raft_peers[i]);
        if (rc)
        {
            DECLARE_AND_INIT_UUID_STR(peer_uuid,
                                      csn_raft->csnr_members[i].csrm_peer);

            LOG_MSG(LL_ERROR,
                    "ctl_svc_node_lookup() failed to find raft-peer UUID=%s\n"
                    "Please check the local-control-service directory: %s",
                    peer_uuid, ctl_svc_get_local_dir());

            goto cleanup;
        }

        DECLARE_AND_INIT_UUID_STR(uuid_str,
                                  csn_raft->csnr_members[i].csrm_peer);

        DBG_CTL_SVC_NODE(LL_NOTIFY, ri->ri_csn_raft,
                         "raft-peer-%hhu %s", i, uuid_str);

        if (!ctl_svc_node_cmp(ri->ri_csn_this_peer, ri->ri_csn_raft_peers[i]))
            this_peer_found_in_raft_node = true;
    }

    if (!this_peer_found_in_raft_node && !raft_instance_is_client(ri))
    {
        rc = -ENODEV;
        goto cleanup;
    }

    return 0;

cleanup:
    raft_net_conf_destroy(ri);
    return rc;
}

int
raft_net_instance_shutdown(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    enum log_level ll = ri->ri_startup_error ? LL_NOTIFY : LL_ERROR;

    int rc = 0;

    int epoll_close_rc = raft_net_epoll_cleanup(ri);
    if (epoll_close_rc)
    {
        SIMPLE_LOG_MSG(ll, "raft_net_epoll_cleanup(): %s",
                       strerror(-epoll_close_rc));
        if (!rc)
            rc = epoll_close_rc;
    }

    int shutdown_cb_rc = ri->ri_shutdown_cb ? ri->ri_shutdown_cb(ri) : 0;
    if (shutdown_cb_rc)
    {
        SIMPLE_LOG_MSG(ll, "ri_shutdown_cb(): %s",
                       strerror(-shutdown_cb_rc));
        if (!rc)
            rc = shutdown_cb_rc;
    }

    int sockets_close_rc = raft_net_sockets_close(ri);
    if (sockets_close_rc)
    {
        SIMPLE_LOG_MSG(ll, "raft_net_sockets_close(): %s",
                       strerror(-sockets_close_rc));
        if (!rc)
            rc = sockets_close_rc;
    }

    int timerfd_close_rc = raft_net_timerfd_close(ri);
    if (timerfd_close_rc)
    {
        SIMPLE_LOG_MSG(ll, "raft_net_timerfd_close(): %s",
                       strerror(-timerfd_close_rc));
        if (!rc)
            rc = timerfd_close_rc;
    }

    raft_net_conf_destroy(ri);

    return rc;
}

static void
raft_net_histogram_setup(struct raft_instance *ri)
{
    for (enum raft_instance_hist_types i = RAFT_INSTANCE_HIST_MIN;
         i < RAFT_INSTANCE_HIST_MAX; i++)
    {
        binary_hist_init(&ri->ri_rihs[i].rihs_bh, 0,
                         RAFT_NET_BINARY_HIST_SIZE);

        ri->ri_rihs[i].rihs_type = i;
    }
}

static void
raft_net_connection_to_csn(struct tcp_mgr_connection *tmc,
                           struct ctl_svc_node **ret)
{
    struct ctl_svc_node_peer *peer = OFFSET_CAST(ctl_svc_node_peer,
                                                 csnp_net_data, tmc);
    *ret = OFFSET_CAST(ctl_svc_node, csn_peer, peer);
}

static tcp_mgr_ctx_int_t
raft_net_peer_tcp_cb(struct tcp_mgr_connection *tmc, char *buf, size_t buf_size,
                     struct raft_instance *ri)
{
    if (!tmc || !buf || !buf_size || !ri)
        return -EINVAL;

    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "buf=%p buf_size=%lu", buf, buf_size);

    // used by server to verify that messages are received from expected IP
    struct sockaddr_in from;
    tcp_setup_sockaddr_in(tmc->tmc_tsh.tsh_ipaddr, ntohs(0), &from);
    from.sin_port = 0;

    size_t header_size = tcp_mgr_connection_header_size_get(tmc);
    if (header_size != sizeof(struct raft_rpc_msg))
        return -EBADMSG;

    if (ri->ri_server_recv_cb)
        ri->ri_server_recv_cb(ri, buf, buf_size, &from);

    return 0;
}

static tcp_mgr_ctx_int_t
raft_net_client_tcp_cb(struct tcp_mgr_connection *tmc, char *buf,
                       size_t buf_size, struct raft_instance *ri)
{
    if (!tmc || !buf || !buf_size || !ri)
        return -EINVAL;

    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "buf=%p buf_size=%lu", buf, buf_size);

    // used by server to verify that messages are received from expected IP
    struct sockaddr_in from;
    tcp_setup_sockaddr_in(tmc->tmc_tsh.tsh_ipaddr, ntohs(0), &from);
    from.sin_port = 0;

    size_t header_size = tcp_mgr_connection_header_size_get(tmc);
    if (header_size != sizeof(struct raft_client_rpc_msg))
        return -EBADMSG;

    if (ri->ri_client_recv_cb)
        ri->ri_client_recv_cb(ri, buf, buf_size, &from);

    return 0;
}

static bool
raft_net_is_client_connection(struct raft_instance *ri,
                              struct ctl_svc_node *csn)
{
    bool csn_is_raft_peer = csn->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER;
    bool this_is_client_node = raft_instance_is_client(ri);

    return this_is_client_node || !csn_is_raft_peer;
}

static size_t
raft_net_connection_header_size(struct raft_instance *ri,
                                struct ctl_svc_node *csn)
{
    bool is_client_cxn = raft_net_is_client_connection(ri, csn);
    size_t header_size = is_client_cxn
        ? sizeof(struct raft_client_rpc_msg)
        : sizeof(struct raft_rpc_msg);

    bool csn_is_raft_peer = csn->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER;
    SIMPLE_LOG_MSG(LL_DEBUG, "raft_net_connection_header_size(): "
                             "remote=%s this=%s header_size=%lu",
                   csn_is_raft_peer ? "PEER" : "CLI",
                   raft_server_state_to_string(ri->ri_state),
                   header_size);

    return header_size;
}

static size_t
raft_net_tcp_handshake_fill(struct raft_instance *ri,
                            struct tcp_mgr_connection *tmc,
                            struct raft_rpc_msg *handshake, size_t size)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    NIOVA_ASSERT(handshake && size == sizeof(struct raft_rpc_msg));

    handshake->rrm_type = RAFT_RPC_MSG_TYPE_ANY;
    handshake->rrm_version = 0;
    handshake->rrm__pad = 0;

    uuid_copy(handshake->rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(handshake->rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    struct ctl_svc_node *csn = NULL;
    raft_net_connection_to_csn(tmc, &csn);
    return raft_net_connection_header_size(ri, csn);
}

static tcp_mgr_ctx_int_t
raft_net_tcp_handshake_cb(struct raft_instance *ri,
                          struct tcp_mgr_connection **tmc_out,
                          size_t *header_size_out,
                          int fd, struct raft_rpc_msg *handshake, size_t size)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    NIOVA_ASSERT(ri && handshake && size == sizeof(struct raft_rpc_msg));

    *tmc_out = NULL;

    int raft_uuid_comp = uuid_compare(handshake->rrm_raft_id,
                                      RAFT_INSTANCE_2_RAFT_UUID(ri));
    if (raft_uuid_comp ||
        handshake->rrm_type != RAFT_RPC_MSG_TYPE_ANY ||
        handshake->rrm_version != 0)
    {
        DECLARE_AND_INIT_UUID_STR(hs_sender_uuid, handshake->rrm_sender_id);
        DECLARE_AND_INIT_UUID_STR(hs_raft_uuid, handshake->rrm_raft_id);

        DBG_RAFT_MSG(LL_ERROR, handshake, "invalid raft handshake from %s",
                     hs_sender_uuid);
        SIMPLE_LOG_MSG(LL_DEBUG, "our raft %s, their raft %s"
                                 " comp=%d type=%d, version=%d",
                       ri->ri_raft_uuid_str, hs_raft_uuid, raft_uuid_comp,
                       handshake->rrm_type,
                       handshake->rrm_version);
        return -EINVAL;
    }

    SIMPLE_LOG_MSG(LL_TRACE, "handshake validated");

    struct ctl_svc_node *csn = NULL;
    ctl_svc_node_lookup(handshake->rrm_sender_id, &csn);
    if (!csn)
    {
        DBG_RAFT_MSG(LL_ERROR, handshake, "invalid connection, fd: %d", fd);
        return -ENOENT;
    }

    *header_size_out = raft_net_connection_header_size(ri, csn);
    *tmc_out = &csn->csn_peer.csnp_net_data;

    return 0;
}

static raft_net_cb_ctx_t
raft_net_connection_getput(struct tcp_mgr_connection *tmc,
                           enum epoll_handle_ref_op op)
{
    NIOVA_ASSERT(tmc);

    struct ctl_svc_node *csn;
    raft_net_connection_to_csn(tmc, &csn);

    if (op == EPH_REF_PUT)
        ctl_svc_node_put(csn);
    else
        ctl_svc_node_get(csn);
}

static tcp_mgr_ctx_ssize_t
raft_net_client_msg_bulk_size_cb(struct tcp_mgr_connection *tmc,
                                 struct raft_client_rpc_msg *msg,
                                 struct raft_instance *ri)
{
    return msg->rcrm_data_size;
}

static tcp_mgr_ctx_ssize_t
raft_net_peer_msg_bulk_size_cb(struct tcp_mgr_connection *tmc,
                               struct raft_rpc_msg *msg,
                               struct raft_instance *ri)
{
    return msg->rrm_type != RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST ? 0
        : msg->rrm_append_entries_request.raerqm_entries_sz;
}

static int
raft_net_csn_setup(struct ctl_svc_node *csn, void *data)
{
    NIOVA_ASSERT(csn && ctl_svc_node_is_peer(csn));

    struct raft_instance *ri = data;
    bool is_client_cxn = raft_net_is_client_connection(ri, csn);
    struct tcp_mgr_instance *tmi = is_client_cxn ? &ri->ri_client_tcp_mgr
                                                 : &ri->ri_peer_tcp_mgr;

    const char *ipaddr = ctl_svc_node_peer_2_ipaddr(csn);
    int port = is_client_cxn ? ctl_svc_node_peer_2_client_port(csn)
                             : ctl_svc_node_peer_2_port(csn);

    tcp_mgr_connection_setup(&csn->csn_peer.csnp_net_data, tmi, ipaddr, port);

    return 0;
}

static void
raft_net_ctl_svc_nodes_setup(struct raft_instance *ri)
{
    // types must match ctl_svc_node_is_peer test
    const int PEER_TYPES_COUNT = 3;
    enum ctl_svc_node_type peer_types[] = {
        CTL_SVC_NODE_TYPE_NIOSD,
        CTL_SVC_NODE_TYPE_RAFT_CLIENT,
        CTL_SVC_NODE_TYPE_RAFT_PEER,
    };

    for (int i = 0; i < PEER_TYPES_COUNT; i++)
        ctl_svc_nodes_apply(peer_types[i], raft_net_csn_setup, ri);
}

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode)
{
    if (!ri)
        return -EINVAL;

    ri->ri_state = client_mode ? RAFT_STATE_CLIENT : RAFT_STATE__NONE;
    ri->ri_proc_state = RAFT_PROC_STATE_BOOTING;
    ri->ri_backend = NULL;

    raft_net_histogram_setup(ri);

    int rc = raft_net_conf_init(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_conf_init(): %s", strerror(-rc));
        return rc;
    }

    if (!raft_net_tcp_disabled())
    {
        tcp_mgr_setup(&ri->ri_client_tcp_mgr, ri,
                      (epoll_mgr_ref_cb_t)raft_net_connection_getput,
                      (tcp_mgr_recv_cb_t)raft_net_client_tcp_cb,
                      (tcp_mgr_bulk_size_cb_t)raft_net_client_msg_bulk_size_cb,
                      (tcp_mgr_handshake_cb_t)raft_net_tcp_handshake_cb,
                      (tcp_mgr_handshake_fill_t)raft_net_tcp_handshake_fill,
                      sizeof(struct raft_rpc_msg),
                      DEFAULT_BULK_CREDITS,
                      DEFAULT_INCOMING_CREDITS);
        tcp_mgr_setup(&ri->ri_peer_tcp_mgr, ri,
                      (epoll_mgr_ref_cb_t)raft_net_connection_getput,
                      (tcp_mgr_recv_cb_t)raft_net_peer_tcp_cb,
                      (tcp_mgr_bulk_size_cb_t)raft_net_peer_msg_bulk_size_cb,
                      (tcp_mgr_handshake_cb_t)raft_net_tcp_handshake_cb,
                      (tcp_mgr_handshake_fill_t)raft_net_tcp_handshake_fill,
                      sizeof(struct raft_rpc_msg),
                      DEFAULT_BULK_CREDITS,
                      DEFAULT_INCOMING_CREDITS);
    }

    rc = raft_net_sockets_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_sockets_setup(): %s",
                       strerror(-rc));
        return rc;
    }

    rc = raft_net_timerfd_create(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_server_timerfd_create(): %s",
                       strerror(-rc));

        raft_net_instance_shutdown(ri);
        return rc;
    }

    rc = raft_net_epoll_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_epoll_setup(): %s", strerror(-rc));

        raft_net_instance_shutdown(ri);
        return rc;
    }

    if (ri->ri_startup_pre_net_bind_cb)
    {
        rc = ri->ri_startup_pre_net_bind_cb(ri);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_WARN, "ri_startup_pre_net_bind_cb(): %s",
                           strerror(-rc));

            if (ri->ri_shutdown_cb)
                ri->ri_shutdown_cb(ri);

            return rc;
        }
    }

    /* bind() after adding the socket to the epoll set.
     */
    rc = raft_net_sockets_bind(ri);

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_sockets_bind(): %s", strerror(-rc));

        if (ri->ri_shutdown_cb)
            ri->ri_shutdown_cb(ri);

        return rc;
    }

    raft_net_ctl_svc_nodes_setup(ri);

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_csn_setup(): %s", strerror(-rc));

        if (ri->ri_shutdown_cb)
            ri->ri_shutdown_cb(ri);

        return rc;
    }

    ri->ri_proc_state = RAFT_PROC_STATE_RUNNING;

    return 0;
}

/**
 * raft_peer_2_idx - attempts to find the peer in the raft_instance
 *    "ri_csn_raft_peers" array.  If found, then the index of the peer is
 *    returned.  The returned index does not pertain to the raft configuration
 *    itself, as the raft config only works from a set of members which are
 *    not specifically labeled numerically.  The use of this function is to
 *    help track this candidate's vote tally.
 */
raft_peer_t
raft_peer_2_idx(const struct raft_instance *ri, const uuid_t peer_uuid)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft);

    const raft_peer_t num_raft_peers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    // Do not tolerate an invalid raft peers number
    NIOVA_ASSERT(num_raft_peers <= CTL_SVC_MAX_RAFT_PEERS);

    for (raft_peer_t i = 0; i < num_raft_peers; i++)
        if (!ctl_svc_node_compare_uuid(ri->ri_csn_raft_peers[i], peer_uuid))
            return i;

    return RAFT_PEER_ANY;
}

/**
 * raft_net_verify_sender_server_msg - verify that an incoming RPC's UUIDs
 *    match what it is expected based on the receiver's config.
 */
struct ctl_svc_node *
raft_net_verify_sender_server_msg(struct raft_instance *ri,
                                  const uuid_t sender_uuid,
                                  const uuid_t sender_raft_uuid,
                                  const struct sockaddr_in *sender_addr)
{
    if (!ri || !sender_uuid || uuid_is_null(sender_uuid))
        return NULL;

    /* Check the id of the sender to make sure they are part of the config
     * and that the RPC is for the correct raft instance.
     */
    const raft_peer_t sender_idx = raft_peer_2_idx(ri, sender_uuid);

    if (sender_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft) ||
        ctl_svc_node_compare_uuid(ri->ri_csn_raft, sender_raft_uuid))
    {
        DECLARE_AND_INIT_UUID_STR(raft_uuid, ri->ri_csn_raft->csn_uuid);
        DECLARE_AND_INIT_UUID_STR(peer_raft_uuid, sender_raft_uuid);

        LOG_MSG(
            LL_NOTIFY,
            "peer not found in my config %hhx %hhx, UUIDs (self=%s, peer=%s)",
            sender_idx, ctl_svc_node_raft_2_num_members(ri->ri_csn_raft),
            raft_uuid, peer_raft_uuid);

        return NULL;
    }

    struct ctl_svc_node *csn = ri->ri_csn_raft_peers[sender_idx];

    if (!ctl_svc_node_is_peer(csn))
        DBG_SIMPLE_CTL_SVC_NODE(LL_FATAL, csn, "csn is not a peer");

    int port = ntohs(sender_addr->sin_port);
    const uint16_t expected_port = (raft_instance_is_client(ri) ?
                                    ctl_svc_node_peer_2_client_port(csn) :
                                    ctl_svc_node_peer_2_port(csn));

    bool udp_ports_match = sender_addr->sin_port == 0 || port == expected_port;
    bool ipaddrs_match = strncmp(ctl_svc_node_peer_2_ipaddr(csn),
                                 inet_ntoa(sender_addr->sin_addr),
                                 IPV4_STRLEN) == 0;

    if (!udp_ports_match || !ipaddrs_match)
    {
        DECLARE_AND_INIT_UUID_STR(peer_uuid, sender_uuid);

        LOG_MSG(LL_NOTIFY,
                "uuid (%s) on unexpected IP:port (%s:%d), expected %s:%hu",
                peer_uuid, inet_ntoa(sender_addr->sin_addr),
                port, ctl_svc_node_peer_2_ipaddr(csn),
                expected_port);

        csn = NULL;
    }
    else if (!net_ctl_can_recv(&csn->csn_peer.csnp_net_ctl))
    {
        // Receive functionality is disabled in the ctl_svc layer.
        DBG_CTL_SVC_NODE(LL_DEBUG, csn, "net_ctl_can_recv() is false");
        csn = NULL;
    }

    return csn;
}

static int
raft_net_send_udp(struct raft_instance *ri, struct ctl_svc_node *csn,
                  struct iovec *iov, size_t niovs,
                  const enum raft_udp_listen_sockets sock_src)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    SIMPLE_LOG_MSG(LL_DEBUG, "ri: %p leader: %p iov: %p no: %lu",
                   ri, ri->ri_csn_leader, iov, niovs);

    if (!ri || !iov || !niovs)
        return -EINVAL;

    else if (niovs > 256)
        return -EMSGSIZE;

    struct sockaddr_in dest;
    int port = sock_src == RAFT_UDP_LISTEN_CLIENT
        ? ctl_svc_node_peer_2_client_port(csn)
        : ctl_svc_node_peer_2_port(csn);
    int rc = udp_setup_sockaddr_in(ctl_svc_node_peer_2_ipaddr(csn), port,
                                   &dest);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "udp_setup_sockaddr_in(): %s (peer=%s:%hu)",
                strerror(-rc), ctl_svc_node_peer_2_ipaddr(csn),
                ctl_svc_node_peer_2_client_port(csn));

        return rc;
    }

    struct udp_socket_handle *ush = &ri->ri_ush[sock_src];

    return udp_socket_send(ush, iov, niovs, &dest);
}

static int
raft_net_send_tcp(struct raft_instance *ri, struct ctl_svc_node *csn,
                  struct iovec *iov, size_t niovs)
{
    SIMPLE_LOG_MSG(LL_TRACE, "ri %p iov %p niovs %ld", ri, iov, niovs);

    if (!ri || !iov || !niovs)
        return -EINVAL;

    else if (niovs > 256)
        return -EMSGSIZE;

    raft_net_update_last_comm_time(ri, csn->csn_uuid, true);

    return tcp_mgr_send_msg(&csn->csn_peer.csnp_net_data, iov, niovs);
}

int
raft_net_send_msg(struct raft_instance *ri, struct ctl_svc_node *csn,
                  struct iovec *iov, size_t niovs,
                  const enum raft_udp_listen_sockets sock_src)
{
    size_t msg_size = niova_io_iovs_total_size_get(iov, niovs);

    SIMPLE_LOG_MSG(LL_DEBUG, "msg_size: %ld udp max: %ld tcp max: %ld",
                   msg_size, udp_get_max_size(), tcp_get_max_size());

    if (msg_size > raft_net_max_rpc_size(ri->ri_store_type))
        return -E2BIG;

    ssize_t size_rc;
    if (!net_ctl_can_send(&csn->csn_peer.csnp_net_ctl))
    {
        DBG_CTL_SVC_NODE(LL_DEBUG, csn, "net_ctl_can_send() is false");
        size_rc = msg_size;
    }
    else
    {
        if (msg_size <= udp_get_max_size())
            size_rc = raft_net_send_udp(ri, csn, iov, niovs, sock_src);
        else if (!raft_net_tcp_disabled() && msg_size <= tcp_get_max_size())
            size_rc = raft_net_send_tcp(ri, csn, iov, niovs);
        else
            size_rc = -E2BIG;
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "raft_net_send_msg(): size_rc=%ld msg_size=%zu",
                   size_rc, msg_size);
    if (size_rc == msg_size)
        raft_net_update_last_comm_time(ri, csn->csn_uuid, true);

    return size_rc == msg_size ? 0 : -ECOMM;
}

int
raft_net_send_msg_to_uuid(struct raft_instance *ri, uuid_t uuid,
                          struct iovec *iov, size_t niovs,
                          const enum raft_udp_listen_sockets sock_src)
{
    struct ctl_svc_node *csn;
    int rc = ctl_svc_node_lookup(uuid, &csn);
    if (rc)
        return rc;

    NIOVA_ASSERT(csn);

    rc = raft_net_send_msg(ri, csn, iov, niovs, sock_src);

    ctl_svc_node_put(csn);

    return rc;
}

int
raft_net_send_client_msg(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!ri || !ri->ri_csn_leader || !rcrm)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "ri %p ldr %p rcrm %p", ri,
                       ri ? ri->ri_csn_leader : 0, rcrm);

        return -EINVAL;
    }

    struct iovec iov = {
        .iov_len = sizeof(struct raft_client_rpc_msg) + rcrm->rcrm_data_size,
        .iov_base = (void *)rcrm,
    };

    return raft_net_send_msg(ri, ri->ri_csn_leader, &iov, 1,
                             RAFT_UDP_LISTEN_CLIENT);
}

int
raft_net_send_client_msgv(struct raft_instance *ri,
                          struct raft_client_rpc_msg *rcrm,
                          const struct iovec *iov, size_t niovs)
{
    SIMPLE_LOG_MSG(LL_TRACE, "rcrm %p iov %p (n=%lu)", rcrm, iov, niovs);

    if (!ri || !ri->ri_csn_leader || !rcrm)
        return -EINVAL;

    else if (niovs > 255 ||
             niova_io_iovs_total_size_get(iov, niovs) != rcrm->rcrm_data_size)
        return -EMSGSIZE;

    struct iovec my_iovs[niovs + 1];
    my_iovs[0].iov_len = sizeof(struct raft_client_rpc_msg);
    my_iovs[0].iov_base = (void *)rcrm;

    // Copy the remaining IOVs into the local iov array
    memcpy(&my_iovs[1], iov, (sizeof(struct iovec) * niovs));

    return raft_net_send_msg(ri, ri->ri_csn_leader, my_iovs, niovs + 1,
                             RAFT_UDP_LISTEN_CLIENT);
}

int
raft_net_verify_sender_client_msg(struct raft_instance *ri,
                                  const uuid_t sender_raft_uuid)
{
    if (!ri || uuid_is_null(sender_raft_uuid))
        return -EINVAL;

    return ctl_svc_node_compare_uuid(ri->ri_csn_raft, sender_raft_uuid) ?
        -ENODEV : 0;
}

/**
 * raft_net_update_last_comm_time - may be used by application level send and
 *     recv handlers to take and record a communication timestamp.
 * @ri:  raft instance pointer
 * @peer_uuid:  server peer uuid (client UUIDs should not be used here).
 * @send_or_recv:  non-zero for 'send'.
 */
void
raft_net_update_last_comm_time(struct raft_instance *ri,
                               const uuid_t peer_uuid, bool send_or_recv)
{
    if (!ri || uuid_is_null(peer_uuid))
        return;

    const raft_peer_t peer_idx = raft_peer_2_idx(ri, peer_uuid);

    if (peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
        return;

    struct timespec *ts = send_or_recv
        ? &ri->ri_last_send[peer_idx]
        : &ri->ri_last_recv[peer_idx];

    // ~1 ms granularity which should be fine for this app.
    niova_realtime_coarse_clock(ts);

    const long long unsigned msec = timespec_2_msec(ts);
    SIMPLE_LOG_MSG(LL_DEBUG,
                   "raft_net_update_last_comm_time(): update %s with ts %llu",
                   send_or_recv  ? "send" : "recv", msec);
}

int
raft_net_comm_get_last_recv(struct raft_instance *ri, const uuid_t peer_uuid,
                            struct timespec *ts)
{
    if (!ri || !ts || uuid_is_null(peer_uuid))
        return -EINVAL;

    const raft_peer_t peer_idx = raft_peer_2_idx(ri, peer_uuid);

    if (peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
        return -ERANGE;

    *ts = ri->ri_last_recv[peer_idx];

    return 0;
}

int
raft_net_comm_recency(const struct raft_instance *ri,
                      raft_peer_t raft_peer_idx,
                      enum raft_net_comm_recency_type type,
                      unsigned long long *ret_ms)
{
    if (!ri || !ri->ri_csn_raft || !ret_ms ||
        raft_peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
        return -EINVAL;

    const unsigned long long last_send =
        timespec_2_msec(&ri->ri_last_send[raft_peer_idx]);
    const unsigned long long last_recv =
        timespec_2_msec(&ri->ri_last_recv[raft_peer_idx]);
    if (!last_send)
        return -EALREADY;

    unsigned long long now = niova_realtime_coarse_clock_get_msec();

    // This should not happen, but just in case..
    if (now < MAX(last_recv, last_send))
        now = MAX(last_recv, last_send);

    int rc = 0;

    switch (type)
    {
    case RAFT_COMM_RECENCY_RECV:
        SIMPLE_LOG_MSG(LL_TRACE, "raft_net_comm_recency(): type recv");
        *ret_ms = last_recv ? (now - last_recv) : 0;
        break;
    case RAFT_COMM_RECENCY_SEND:
        SIMPLE_LOG_MSG(LL_TRACE, "raft_net_comm_recency(): type send");
        *ret_ms = last_send ? (now - last_send) : 0;
        break;
    case RAFT_COMM_RECENCY_UNACKED_SEND:
        SIMPLE_LOG_MSG(LL_TRACE, "raft_net_comm_recency(): type unacked send");
        *ret_ms = (last_send > last_recv) ? (now - last_recv) : 0;
        break;
    default:
        rc = -EINVAL;
        break;
    }

    LOG_MSG(LL_TRACE, "raft_net_comm_recency(): idx=%d now=%llu ms=%llu rc=%d",
            raft_peer_idx, now, *ret_ms, rc);

    return rc;
}

raft_peer_t
raft_net_get_most_recently_responsive_server(const struct raft_instance *ri)
{
    const raft_peer_t nraft_servers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    raft_peer_t start_peer = random_get() % nraft_servers;
    raft_peer_t best_peer = start_peer;

    unsigned long long recency_value = 0;

    for (raft_peer_t i = 0; i < nraft_servers; i++)
    {
        raft_peer_t idx = (i + start_peer) % nraft_servers;
        unsigned long long since_last_recv = 0;

        int rc = raft_net_comm_recency(ri, idx, RAFT_COMM_RECENCY_RECV,
                                       &since_last_recv);
        if (rc == -EALREADY)
            continue;

        SIMPLE_LOG_MSG(LL_NOTIFY,
                       "raft_net_comm_recency(): idx=%d since_last_recv=%llu",
                       idx, since_last_recv);

        FATAL_IF((rc), "raft_net_comm_recency(): %s", strerror(-rc));

        if (since_last_recv < recency_value)
        {
            best_peer = idx;
            recency_value = since_last_recv;
        }
    }

    return best_peer;
}

static int
raft_net_server_target_check_by_idx(const struct raft_instance *ri,
                                    const raft_peer_t idx,
                                    const unsigned long long stale_timeout_ms)
{
    unsigned long long recency_ms = 0;

    int rc = raft_net_comm_recency(ri, idx, RAFT_COMM_RECENCY_UNACKED_SEND,
                                   &recency_ms);

    if (!rc)
        return (recency_ms > stale_timeout_ms) ? -ETIMEDOUT : 0;

    return rc;

}

int
raft_net_server_target_check(const struct raft_instance *ri,
                             const uuid_t server_uuid,
                             const unsigned long long stale_timeout_ms)
{
    return raft_net_server_target_check_by_idx(
        ri, raft_peer_2_idx(ri, server_uuid), stale_timeout_ms);
}

int
raft_net_apply_leader_redirect(struct raft_instance *ri,
                               const uuid_t redirect_target,
                               const unsigned long long stale_timeout_ms)
{
    if (!ri || uuid_is_null(redirect_target))
        return -EINVAL;

    raft_peer_t leader_idx = raft_peer_2_idx(ri, redirect_target);
    if (leader_idx == RAFT_PEER_ANY)
        return -ENOENT;

    // raft_peer_2_idx() should not return an out-of-bounds value.
    NIOVA_ASSERT(leader_idx < CTL_SVC_MAX_RAFT_PEERS);

    ri->ri_csn_leader = ri->ri_csn_raft_peers[leader_idx];

    int rc = raft_net_server_target_check_by_idx(ri, leader_idx,
                                                 stale_timeout_ms);

    if (rc == -ETIMEDOUT)
        timespec_clear(&ri->ri_last_send[leader_idx]); // "unstale" the leader

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "new leader via redirect (idx=%hhu)",
                      leader_idx);

    return 0;
}

void
raft_net_timerfd_settime(struct raft_instance *ri, unsigned long long msecs)
{
    struct itimerspec its = {0};

    msec_2_timespec(&its.it_value, msecs);

    int rc = timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);

    FATAL_IF((rc), "timerfd_settime(): %s", strerror(errno));
}

raft_net_timerfd_cb_ctx_t
raft_net_timerfd_cb(const struct epoll_handle *eph, uint32_t events)
{
    struct raft_instance *ri = eph->eph_arg;

    ssize_t rc = niova_io_fd_drain(ri->ri_timer_fd, NULL);
    if (rc)
    {
        // Something went awry with the timerfd read.
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "niova_io_fd_drain(): %zd", rc);
        return;
    }

    if (ri->ri_ignore_timerfd)
        raft_net_timerfd_settime(ri, 1);

    else if (ri->ri_timer_fd_cb)
        ri->ri_timer_fd_cb(ri);
}

static enum raft_udp_listen_sockets
raft_net_udp_identify_socket(const struct raft_instance *ri, const int fd)
{
    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX; i++)
        if (udp_socket_handle_2_sockfd(&ri->ri_ush[i]) == fd)
            return i;

    return RAFT_UDP_LISTEN_ANY;
}

/**
 * raft_net_udp_cb - this is the receive handler for all incoming UDP
 *    requests and replies.  The program is single threaded so the msg sink
 *    buffers are allocated statically here.  Operations that can be handled
 *    from this callback are:  client RPC requests, vote requests (if
 *    peer is candidate), vote replies (if self is candidate).
 */
static raft_net_cb_ctx_t
raft_net_udp_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    static char sink_buf[NIOVA_MAX_UDP_SIZE];
    static struct sockaddr_in from;
    static struct iovec iovs[1] = {
        [0].iov_base = (void *)sink_buf,
        [0].iov_len  = NIOVA_MAX_UDP_SIZE,
    };

    NIOVA_ASSERT(eph && eph->eph_arg);

    struct raft_instance *ri = eph->eph_arg;
    NIOVA_ASSERT(ri);

    /* Clear the fd descriptor before doing any other error checks on the
     * sender.
     */
    ssize_t recv_bytes =
        udp_socket_recv_fd(eph->eph_fd, iovs, 1, &from, false);

    if (recv_bytes < 0) // return from a general recv error
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "udp_socket_recv_fd():  %s",
                          strerror(-recv_bytes));
        return;
    }

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "fd=%d type=%d rc=%zd",
                      eph->eph_fd,
                      raft_net_udp_identify_socket(ri, eph->eph_fd),
                      recv_bytes);

    switch (raft_net_udp_identify_socket(ri, eph->eph_fd))
    {
    case RAFT_UDP_LISTEN_SERVER:
        if (ri->ri_server_recv_cb)
            ri->ri_server_recv_cb(ri, sink_buf, recv_bytes, &from);
        break;
    case RAFT_UDP_LISTEN_CLIENT:
        if (ri->ri_client_recv_cb)
            ri->ri_client_recv_cb(ri, sink_buf, recv_bytes, &from);
        break;
    default:
        break;
    }
}

/**
 * raft_net_write_supp_get - looks up a supplment pointer base on the value
 *    of the provided handle.  Note, that a NULL handle is permitted.
 */
static struct raft_net_wr_supp *
raft_net_write_supp_get(struct raft_net_sm_write_supplements *rnsws,
                        void *handle)
{
    NIOVA_ASSERT(rnsws->rnsws_nitems < RAFT_NET_WR_SUPP_MAX);

    for (size_t i = 0; i < rnsws->rnsws_nitems; i++)
        if (handle == rnsws->rnsws_ws[i].rnws_handle)
            return &rnsws->rnsws_ws[i];

    if (rnsws->rnsws_nitems == RAFT_NET_WR_SUPP_MAX)
        return NULL;

    const size_t idx = rnsws->rnsws_nitems;

    int rc = niova_reallocarray(rnsws->rnsws_ws, struct raft_net_wr_supp,
                                idx + 1UL);
    if (rc)
        return NULL;

    rnsws->rnsws_nitems++;

    // Initialize pointers to NULL
    memset(&rnsws->rnsws_ws[idx], 0, sizeof(struct raft_net_wr_supp));

    rnsws->rnsws_ws[idx].rnws_handle = handle;

    return &rnsws->rnsws_ws[idx];
}

static void
raft_net_write_supp_destroy(struct raft_net_wr_supp *ws, bool complete_destroy)
{
    if (!ws || !ws->rnws_nkv)
        return;

    if (complete_destroy)
    {
        for (size_t i = 0; i < ws->rnws_nkv; i++)
        {
            niova_free(ws->rnws_keys[i]);
            niova_free(ws->rnws_values[i]);
        }
    }

    ws->rnws_nkv = 0;

    niova_free(ws->rnws_keys);
    niova_free(ws->rnws_values);
    niova_free(ws->rnws_key_sizes);
    niova_free(ws->rnws_value_sizes);

    if (complete_destroy && ws->rnws_comp_cb)
        ws->rnws_comp_cb(ws->rnws_handle);
}

static int
raft_net_write_supp_add(struct raft_net_wr_supp *ws, const char *key,
                        const size_t key_size, const char *value,
                        const size_t value_size)
{
    if (!ws || !key || !key_size)
        return -EINVAL;

    else if (ws->rnws_nkv == RAFT_NET_WR_SUPP_MAX)
        return -ENOSPC;

    NIOVA_ASSERT(ws->rnws_nkv < RAFT_NET_WR_SUPP_MAX);

    size_t n = ws->rnws_nkv;

    int rc = niova_reallocarray(ws->rnws_keys, char *, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(ws->rnws_key_sizes, size_t, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(ws->rnws_values, char *, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(ws->rnws_value_sizes, size_t, n + 1UL);
    if (rc)
        return rc;

    ws->rnws_keys[n] = niova_malloc(key_size);
    if (!ws->rnws_keys[n])
        return -ENOMEM;

    ws->rnws_values[n] = niova_malloc(value_size);
    if (!ws->rnws_values[n])
    {
        niova_free(ws->rnws_keys[n]);
        return -ENOMEM;
    }

    memcpy(ws->rnws_keys[n], key, key_size);
    memcpy(ws->rnws_values[n], value, value_size);

    ws->rnws_key_sizes[n] = key_size;
    ws->rnws_value_sizes[n] = value_size;

    ws->rnws_nkv++;

    LOG_MSG(LL_DEBUG, "ws=%p nkv=%zu key=%s val=%s", ws, ws->rnws_nkv, key,
            value);

    return 0;
}

/*
 * Append the write supplement elements from source ws to desc ws.
 **/
int
raft_net_sm_write_supplement_merge(struct raft_net_sm_write_supplements *dest,
                                   struct raft_net_sm_write_supplements *src)
{

    struct raft_net_wr_supp *dest_ws = raft_net_write_supp_get(dest,
                                        (void *)src->rnsws_ws->rnws_handle);
    size_t n = dest_ws->rnws_nkv;

    int rc = niova_reallocarray(dest_ws->rnws_keys, char *, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(dest_ws->rnws_key_sizes, size_t, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(dest_ws->rnws_values, char *, n + 1UL);
    if (rc)
        return rc;

    rc = niova_reallocarray(dest_ws->rnws_value_sizes, size_t, n + 1UL);
    if (rc)
        return rc;

    struct raft_net_wr_supp *src_ws = src->rnsws_ws;

    dest_ws->rnws_keys[n] = src_ws->rnws_keys[0];
	dest_ws->rnws_values[n] = src_ws->rnws_values[0];
    dest_ws->rnws_key_sizes[n] = src_ws->rnws_key_sizes[0];
    dest_ws->rnws_value_sizes[n] = src_ws->rnws_value_sizes[0];
    dest_ws->rnws_nkv++;
    SIMPLE_LOG_MSG(LL_DEBUG, "Merging ws : nkv: %ld, key: %s, value: %s, key_size: %ld,"
                            " value_size: %ld", n, dest_ws->rnws_keys[n],
                            dest_ws->rnws_values[n], dest_ws->rnws_key_sizes[n],
                            dest_ws->rnws_value_sizes[n]);

   return 0;
}

int
raft_net_client_user_id_parse(const char *in,
                              struct raft_net_client_user_id *rncui,
                              const version_t version)
{
    if (!in || !rncui)
        return -EINVAL;

    else if (version != 0)
        return -EOPNOTSUPP;

    // An otherwise invalid string could appear valid after the below strncpy
    else if (strnlen(in, RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE) ==
             RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE)
        return -ENAMETOOLONG;

    char local_str[RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE];
    strncpy(local_str, in, RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE - 1);
    local_str[RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE - 1] = '\0';

    const char *uuid_str = NULL;

    int rc = regexec(&raftNetRncuiRegex, local_str, 0, NULL, 0);
    if (!rc)
    {
        const char *sep = RAFT_NET_CLIENT_USER_ID_V0_STR_SEP;
        char *sp = NULL;
        char *sub;

        size_t pos = 0;
        for (sub = strtok_r(local_str, sep, &sp);
             sub != NULL;
             sub = strtok_r(NULL, sep, &sp), pos++)
        {
            if (!pos)
            {
                rc = uuid_parse(sub, RAFT_NET_CLIENT_USER_ID_2_UUID(rncui, 0,
                                                                    0));
                if (rc)
                {
                    rc = -EINVAL;
                    break;
                }

                uuid_str = sub;
            }
            else
            {
                NIOVA_ASSERT((1 + pos) < RAFT_NET_CLIENT_USER_ID_V0_NUINT64);

                RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, 0, 1 + pos) =
                    strtoull(sub, NULL, 16);
            }
        }
    }

    if (!rc)
        SIMPLE_LOG_MSG(LL_DEBUG, RAFT_NET_CLIENT_USER_ID_FMT,
                       RAFT_NET_CLIENT_USER_ID_FMT_ARGS(rncui, uuid_str, 0));
    else
        LOG_MSG(LL_ERROR, "parse failed for `%s'", local_str);

    return rc;
}

/**
 * raft_net_sm_write_supplement_add - 'write supplements' are KVs which
 *    accompany a backend write operation and are atomically applied to the
 *    backend with that write operation.  There are 2 cases where supplements
 *    are used.  First, is the writing of a raft-log-entry, here, the state-
 *    machine (SM) may require that certain KVs are emplaced at the time of the
 *    log entry write.  The second case occurs when a raft instance calls out
 *    to the SM to apply a committed raft-log-entry.  Here raft itself may wish
 *    to update its state reflecting the SM apply operation.
 */
int
raft_net_sm_write_supplement_add(struct raft_net_sm_write_supplements *rnsws,
                                 void *handle, void (*rnws_comp_cb)(void *),
                                 const char *key, const size_t key_size,
                                 const char *value, const size_t value_size)
{
    if (!rnsws || !key || !key_size)
	{
		SIMPLE_LOG_MSG(LL_WARN, "Return error EINVAL");
        return -EINVAL;
	}

    struct raft_net_wr_supp *ws = raft_net_write_supp_get(rnsws, handle);
    if (!ws)
	{
		SIMPLE_LOG_MSG(LL_WARN, "Return error ENOMEM");
        return -ENOMEM;
	}

    if (rnws_comp_cb) // Apply the callback if it was specified
        ws->rnws_comp_cb = rnws_comp_cb;

    return raft_net_write_supp_add(ws, key, key_size, value, value_size);
}

void
raft_net_sm_write_supplement_destroy(
    struct raft_net_sm_write_supplements *rnsws,
    bool complete_destroy)
{
    if (!rnsws || !rnsws->rnsws_nitems)
        return;

    for (size_t i = 0; i < rnsws->rnsws_nitems; i++)
        raft_net_write_supp_destroy(&rnsws->rnsws_ws[i], complete_destroy);

    niova_free(rnsws->rnsws_ws);

    rnsws->rnsws_ws = NULL;
    rnsws->rnsws_nitems = 0;
}

void
raft_net_sm_write_supplement_init(struct raft_net_sm_write_supplements *rnsws)
{
    if (rnsws)
        rnsws->rnsws_nitems = 0;
}

void
raft_net_instance_apply_callbacks(struct raft_instance *ri,
                                  raft_net_timer_cb_t timer_fd_cb,
                                  raft_net_cb_t client_recv_cb,
                                  raft_net_cb_t server_recv_cb)
{
    NIOVA_ASSERT(ri);

    ri->ri_timer_fd_cb = timer_fd_cb;
    ri->ri_client_recv_cb = client_recv_cb;
    ri->ri_server_recv_cb = server_recv_cb;
}

int entry_cnt;
static init_ctx_t NIOVA_CONSTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
raft_net_init(void)
{
    FUNC_ENTRY(LL_NOTIFY);

    if (entry_cnt++ > 0)
        return;

    LREG_ROOT_OBJECT_ENTRY_INSTALL_RESCAN_LCTLI(raft_net_info);
    LREG_ROOT_OBJECT_ENTRY_INSTALL_RESCAN_LCTLI(raft_net_bulk_recovery_info);

    int rc = regcomp(&raftNetRncuiRegex, RNCUI_V0_REGEX_BASE, 0);
    NIOVA_ASSERT(!rc);

    FUNC_EXIT(LL_NOTIFY);

    return;
}

static destroy_ctx_t NIOVA_DESTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
raft_net_destroy(void)
{
    regfree(&raftNetRncuiRegex);
}
