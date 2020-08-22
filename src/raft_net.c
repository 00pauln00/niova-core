/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/timerfd.h>
#include <linux/limits.h>
#include <string.h>


#include "alloc.h"
#include "crc32.h"
#include "ctl_svc.h"
#include "ctor.h"
#include "epoll_mgr.h"
#include "init.h"
#include "io.h"
#include "log.h"
#include "raft.h"
#include "random.h"
#include "udp.h"
#include "util_thread.h"

enum raft_net_lreg_values
{
    RAFT_NET_LREG_IGNORE_TIMER_EVENTS,
    RAFT_NET_LREG__MAX,
};

struct raft_instance raftInstance = {
//    .ri_store_type = RAFT_INSTANCE_STORE_ROCKSDB,
    .ri_store_type = RAFT_INSTANCE_STORE_POSIX_FLAT_FILE,
};

REGISTRY_ENTRY_FILE_GENERATE;

static util_thread_ctx_reg_int_t
raft_net_lreg_multi_facet_cb(enum lreg_node_cb_ops, struct lreg_value *,
                             void *);

LREG_ROOT_ENTRY_GENERATE(raft_net, LREG_USER_TYPE_RAFT_NET);

LREG_ROOT_ENTRY_GENERATE_OBJECT(raft_net_info, LREG_USER_TYPE_RAFT_NET,
                                RAFT_NET_LREG__MAX,
                                raft_net_lreg_multi_facet_cb, NULL);

struct raft_instance *
raft_net_get_instance(void)
{
    return &raftInstance;
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
        default:
            rc = -ENOENT;
            break;
        }
        break;
    case LREG_NODE_CB_OP_WRITE_VAL:
        if (lv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lv), &tmp_bool);
        if (rc)
            return rc;

        switch (lv->lrv_value_idx_in)
        {
        case RAFT_NET_LREG_IGNORE_TIMER_EVENTS:
            ri->ri_ignore_timerfd = tmp_bool;
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

static int
raft_net_tcp_sockets_close(struct raft_instance *ri)
{
    int rc = tcp_socket_close(&ri->ri_listen_socket);

    return rc;
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
    rc2 = raft_net_tcp_sockets_close(ri);

    if (rc)
        return rc;

    return rc2;
}

static int
raft_net_tcp_sockets_bind(struct raft_instance *ri)
{
    int rc = tcp_socket_bind(&ri->ri_listen_socket);
    if (rc)
        raft_net_tcp_sockets_close(ri);

    return rc;
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
    int rc;

    rc = raft_net_udp_sockets_bind(ri);
    if (rc)
        return rc;

    rc = raft_net_tcp_sockets_bind(ri);

    return rc;
}

static int
raft_net_tcp_sockets_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    strncpy(ri->ri_listen_socket.tsh_ipaddr,
            ctl_svc_node_peer_2_ipaddr(ri->ri_csn_this_peer), IPV4_STRLEN);
    ri->ri_listen_socket.tsh_port =
        ctl_svc_node_peer_2_port(ri->ri_csn_this_peer);
    int rc = tcp_socket_setup(&ri->ri_listen_socket);

    return rc;
}

static int
raft_net_udp_sockets_setup(struct raft_instance *ri)
{
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
    if (rc)
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
        ri->ri_timer_fd = -1;
        return close(ri->ri_timer_fd);
    }

    return 0;
}

static int
raft_net_epoll_cleanup(struct raft_instance *ri)
{
    for (enum raft_epoll_handles i = 0; i < RAFT_EPOLL_HANDLES_MAX; i++)
        epoll_handle_del(&ri->ri_epoll_mgr, &ri->ri_epoll_handles[i]);

    return epoll_mgr_close(&ri->ri_epoll_mgr);
}

/**
 *  raft_net_udp_cb - forward declaration for the generic udp recv handler.
 */
static raft_net_cb_ctx_t
raft_net_udp_cb(const struct epoll_handle *, uint32_t events);

static raft_net_cb_ctx_t
raft_net_tcp_listen_cb(const struct epoll_handle *, uint32_t events);

static int
raft_net_epoll_handle_add(struct raft_instance *ri, int fd, epoll_mgr_cb_t cb)
{
    if (!ri || fd < 0 || !cb)
        return -EINVAL;

    else if (ri->ri_epoll_handles_in_use >= RAFT_EPOLL_HANDLES_MAX)
        return -ENOSPC;

    size_t idx = ri->ri_epoll_handles_in_use++;

    int rc =
        epoll_handle_init(&ri->ri_epoll_handles[idx], fd, EPOLLIN, cb, ri);

    return rc ? rc :
	epoll_handle_add(&ri->ri_epoll_mgr, &ri->ri_epoll_handles[idx]);
}

static int
raft_epoll_setup_udp(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = 0;

    /* Next, add the udp sockets.
     */
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
raft_epoll_setup_tcp(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    return raft_net_epoll_handle_add(ri, ri->ri_listen_socket.tsh_socket,
                                     raft_net_tcp_listen_cb);
}

static int
raft_epoll_setup_net(struct raft_instance *ri)
{
    int rc;

    rc = raft_epoll_setup_udp(ri);
    if (rc)
        return rc;

    rc = raft_epoll_setup_tcp(ri);

    return rc;
}

int
raft_net_evp_add(struct raft_instance *ri, epoll_mgr_cb_t cb)
{
    if (!ri || !cb)
        return -EINVAL;

    else if (ri->ri_epoll_handles_in_use >= RAFT_EPOLL_HANDLES_MAX ||
             ri->ri_evps_in_use >= RAFT_EVP_HANDLES_MAX)
        return -ENOSPC;

    int idx = ri->ri_evps_in_use++;

    struct ev_pipe *evp = &ri->ri_evps[idx];

    int rc = ev_pipe_setup(evp);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "ev_pipe_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = raft_net_epoll_handle_add(ri, evp_read_fd_get(evp), cb);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_epoll_handle_add(): %s",
                       strerror(-rc));
        return rc;
    }

    evp_increment_reader_cnt(evp); //Xxx this is a mess
    // should be inside ev_pipe.c!

    return idx;
}

raft_net_timerfd_cb_ctx_t
raft_net_timerfd_cb(const struct epoll_handle *, uint32_t events);

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

    int epoll_close_rc = raft_net_epoll_cleanup(ri);

    if (!raft_instance_is_client(ri))
        raft_server_instance_shutdown(ri);

    int sockets_close = raft_net_sockets_close(ri);

    raft_net_timerfd_close(ri);

    raft_net_conf_destroy(ri);

    return (sockets_close ? sockets_close :
            (epoll_close_rc ? epoll_close_rc : 0));
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

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode)
{
    if (!ri)
        return -EINVAL;

    ri->ri_state = client_mode ? RAFT_STATE_CLIENT : RAFT_STATE_BOOTING;

    raft_net_histogram_setup(ri);

    int rc = raft_net_conf_init(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_conf_init(): %s", strerror(-rc));
        return rc;
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

    if (!client_mode)
        raft_server_instance_init(ri);

    rc = raft_net_epoll_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_epoll_setup(): %s", strerror(-rc));

        raft_net_instance_shutdown(ri);
        return rc;
    }

    if (!client_mode)
    {
        rc = raft_server_instance_startup(ri);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_WARN, "raft_server_instance_startup(): %s",
                           strerror(-rc));

            raft_server_instance_shutdown(ri);

            return rc;
        }
    }

    /* bind() after adding the socket to the epoll set.
     */
    rc = raft_net_sockets_bind(ri);

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_sockets_bind(): %s", strerror(-rc));
        raft_net_instance_shutdown(ri);
        return rc;
    }

    return 0;
}

int
raft_net_server_instance_run(const char *raft_uuid_str,
                             const char *this_peer_uuid_str,
                             raft_sm_request_handler_t sm_request_handler,
                             enum raft_instance_store_type type)
{
    if (!raft_uuid_str || !this_peer_uuid_str || !sm_request_handler)
        return -EINVAL;

    struct raft_instance *ri = raft_net_get_instance();

    ri->ri_raft_uuid_str = raft_uuid_str;
    ri->ri_this_peer_uuid_str = this_peer_uuid_str;
    ri->ri_server_sm_request_cb = sm_request_handler;

    raft_instance_backend_type_specify(ri, type);

    int rc = raft_net_instance_startup(ri, false);
    if (rc)
        return rc;

    rc = raft_server_main_loop(ri);

    raft_net_instance_shutdown(ri);

    return rc;
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
    NIOVA_ASSERT(ri && ri->ri_csn_raft && ri->ri_csn_raft_peers);

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

        SIMPLE_LOG_MSG(LL_NOTIFY, "peer not found in my config %hhx %hhx",
                       sender_idx,
                       ctl_svc_node_raft_2_num_members(ri->ri_csn_raft));
        SIMPLE_LOG_MSG(LL_NOTIFY, "my-raft=%s peer-raft=%s",
                       raft_uuid, peer_raft_uuid);
        return NULL;
    }

    struct ctl_svc_node *csn = ri->ri_csn_raft_peers[sender_idx];

    if (!ctl_svc_node_is_peer(csn))
        DBG_SIMPLE_CTL_SVC_NODE(LL_FATAL, csn, "csn is not a peer");

    const uint16_t expected_port = (raft_instance_is_client(ri) ?
                                    ctl_svc_node_peer_2_client_port(csn) :
                                    ctl_svc_node_peer_2_port(csn));

    // XXX this check only works on UDP
    if (false &&
        ((sender_addr->sin_port) != expected_port ||
        strncmp(ctl_svc_node_peer_2_ipaddr(csn),
                inet_ntoa(sender_addr->sin_addr), IPV4_STRLEN)))
    {
        LOG_MSG(LL_NOTIFY, "uuid (%s) on unexpected IP:port (%s:%hu)",
                sender_uuid, inet_ntoa(sender_addr->sin_addr), expected_port);

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

int
raft_net_send_client_msg_udp(struct raft_instance *ri,
                             struct raft_client_rpc_msg *rcrm)
{
    if (!ri || !ri->ri_csn_leader || !rcrm)
        return -EINVAL;

    const ssize_t msg_size =
        sizeof(struct raft_client_rpc_msg) + rcrm->rcrm_data_size;

    if (msg_size > NIOVA_MAX_UDP_SIZE)
        return -E2BIG;

    struct ctl_svc_node *csn = ri->ri_csn_leader;
    struct sockaddr_in dest;

    int rc = udp_setup_sockaddr_in(ctl_svc_node_peer_2_ipaddr(csn),
                                   ctl_svc_node_peer_2_client_port(csn),
                                   &dest);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "udp_setup_sockaddr_in(): %s (peer=%s:%hu)",
                strerror(-rc), ctl_svc_node_peer_2_ipaddr(csn),
                ctl_svc_node_peer_2_client_port(csn));

        return rc;
    }

    struct udp_socket_handle *ush = &ri->ri_ush[RAFT_UDP_LISTEN_CLIENT];

    struct iovec iov[1] = {
        [0].iov_len = msg_size,
        [0].iov_base = (void *)rcrm,
    };

    ssize_t size_rc;
    if (!net_ctl_can_send(&csn->csn_peer.csnp_net_ctl))
    {
        DBG_CTL_SVC_NODE(LL_DEBUG, csn, "net_ctl_can_send() is false");
        size_rc = msg_size;
    }
    else
    {
        size_rc = udp_socket_send(ush, iov, 1, &dest);
    }

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, rcrm, &dest, "size-rc=%zd", size_rc);

    if (size_rc == msg_size)
        raft_net_update_last_comm_time(ri, csn->csn_uuid, true);

    return size_rc == msg_size ? 0 : -ECOMM;
}

int
raft_net_send_client_msg_tcp(struct raft_instance *ri,
                             struct raft_client_rpc_msg *rcrm)
{
    if (!ri || !ri->ri_csn_leader || !rcrm)
        return -EINVAL;

    const ssize_t msg_size =
        sizeof(struct raft_client_rpc_msg) + rcrm->rcrm_data_size;

    if (msg_size > RAFT_NET_MAX_RPC_SIZE)
        return -E2BIG;

    struct ctl_svc_node *csn = ri->ri_csn_leader;
    struct raft_net_connection *rnc = raft_net_remote_connect(ri, csn);
    if (!rnc)
    {
        return -ECOMM;
    }

    struct iovec iov = {
        .iov_len = msg_size,
        .iov_base = (void *)rcrm,
    };

    ssize_t size_rc;
    if (!net_ctl_can_send(&csn->csn_peer.csnp_net_ctl))
    {
        DBG_CTL_SVC_NODE(LL_DEBUG, csn, "net_ctl_can_send() is false");
        size_rc = msg_size;
    }
    else
    {
        size_rc = tcp_socket_send(&rnc->rnc_tsh, &iov, 1);
    }

    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_send_client_msg_tcp(): size_rc=%zu msg_size=%zu", size_rc, msg_size);
    if (size_rc == msg_size)
        raft_net_update_last_comm_time(ri, csn->csn_uuid, true);

    return size_rc == msg_size ? 0 : -ECOMM;
}

int
raft_net_send_client_msg(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm)
{
    int rc = raft_net_send_client_msg_udp(ri, rcrm);
    if (rc == -E2BIG)
        return raft_net_send_client_msg_tcp(ri, rcrm);
    else
        return rc;
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

    struct timespec *ts = send_or_recv ?
        &ri->ri_last_send[peer_idx] : &ri->ri_last_recv[peer_idx];

    const long long unsigned msec = timespec_2_msec(ts);

    // ~1 ms granularity which should be fine for this app.
    niova_realtime_coarse_clock(ts);
    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_update_last_comm_time(): update %s with ts %llu",
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
    {
        return -EALREADY;
    }

    unsigned long long now = niova_realtime_coarse_clock_get_msec();

    // This should not happen, but just in case..
    if (now < MAX(last_recv, last_send))
        now = MAX(last_recv, last_send);

    int rc = 0;

    switch (type)
    {
    case RAFT_COMM_RECENCY_RECV:
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_comm_recency(): type recv");
        *ret_ms = last_recv ? (now - last_recv) : 0;
        break;
    case RAFT_COMM_RECENCY_SEND:
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_comm_recency(): type send");
        *ret_ms = last_send ? (now - last_send) : 0;
        break;
    case RAFT_COMM_RECENCY_UNACKED_SEND:
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_comm_recency(): type unacked send");
        *ret_ms = (last_send > last_recv) ? (now - last_recv) : 0;
        break;
    default:
        rc = -EINVAL;
        break;
    }

    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_comm_recency(): idx=%d now=%llu ms=%llu rc=%d", raft_peer_idx, now, *ret_ms, rc);

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
        {
            continue;
        }

        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_comm_recency(): idx=%d since_last_recv=%llu", idx, since_last_recv);

        FATAL_IF((rc), "raft_net_comm_recency(): %s", strerror(-rc));

        if (since_last_recv < recency_value)
        {
            best_peer = idx;
            recency_value = since_last_recv;
        }
    }

    return best_peer;
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

    ssize_t rc = io_fd_drain(ri->ri_timer_fd, NULL);
    if (rc)
    {
        // Something went awry with the timerfd read.
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "io_fd_drain(): %zd", rc);
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

static raft_net_cb_ctx_t
raft_net_tcp_handshake_cb(const struct epoll_handle *eph, uint32_t events);

void
raft_net_connection_setup(struct raft_instance *ri, struct raft_net_connection *rnc)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);

    tcp_socket_handle_init(&rnc->rnc_tsh);
    rnc->rnc_eph.eph_installed = 0;

    rnc->rnc_ri = ri;
    rnc->rnc_status = RNCS_DISCONNECTED;

    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_connection_setup() - rnc %p", rnc);
}

static void
raft_net_connection_close(struct raft_net_connection *rnc)
{
    if (rnc->rnc_eph.eph_installed)
        epoll_handle_del(&rnc->rnc_ri->ri_epoll_mgr, &rnc->rnc_eph);

    tcp_socket_close(&rnc->rnc_tsh);
}

static struct raft_net_connection *
raft_net_tcp_incoming_new(struct raft_instance *ri)
{
    struct raft_net_connection *rnc = niova_malloc(sizeof(struct raft_net_connection));
    if (!rnc)
    {
        return NULL;
    }

    raft_net_connection_setup(ri, rnc);

    return rnc;
}

static void
raft_net_tcp_incoming_fini(struct raft_net_connection *rnc)
{
    raft_net_connection_close(rnc);
    niova_free(rnc);
}

static int
raft_net_tcp_accept(struct raft_instance *ri, int fd)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);

    struct raft_net_connection *rnc = raft_net_tcp_incoming_new(ri);
    if (!rnc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_tcp_incoming_new(): NOMEM");
        return -ENOMEM;
    }

    int rc = tcp_socket_handle_accept(fd, &rnc->rnc_tsh);
    if (rc < 0) {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_handle_accept(): %d", rc);
        goto err1;
    }

    // set in handle_accept
    int new_fd = rnc->rnc_tsh.tsh_socket;

    SIMPLE_LOG_MSG(LL_NOTIFY, "waiting for handshake fd:%d", new_fd);
    rc = epoll_handle_init(&rnc->rnc_eph, new_fd, EPOLLIN, raft_net_tcp_handshake_cb, rnc);
    if (rc < 0) {
        SIMPLE_LOG_MSG(LL_ERROR, "epoll_handle_init(): %d", rc);
        goto err1;
    }

    rc = epoll_handle_add(&rnc->rnc_ri->ri_epoll_mgr, &rnc->rnc_eph);
    if (rc < 0) {
        SIMPLE_LOG_MSG(LL_ERROR, "epoll_handle_add(): %s (%d)", strerror(rc), rc);
        goto err1;
    }

    return 0;

err1:
    raft_net_tcp_incoming_fini(rnc);

    return rc;
}

static int
raft_net_tcp_handshake_send(struct raft_net_connection *rnc)
{
    static struct raft_rpc_msg handshake = {
        // XXX should there be a new type?
        .rrm_type = RAFT_RPC_MSG_TYPE_ANY,
        .rrm_version = 0,
        .rrm__pad = 0,
    };

    static struct iovec iov = {
        .iov_base = (void *)&handshake,
        .iov_len  = sizeof(handshake),
    };

    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_handshake_send()");

    uuid_copy(handshake.rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(rnc->rnc_ri));
    uuid_copy(handshake.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(rnc->rnc_ri));

    size_t size_rc = tcp_socket_send(&rnc->rnc_tsh, &iov, 1);

    return size_rc;
}

static int
epoll_handle_rc_get(const struct epoll_handle *eph, uint32_t events)
{
    int rc = 0;

    if (events & (EPOLLHUP | EPOLLERR))
    {
        socklen_t rc_len = sizeof(rc);
        int rc2 = getsockopt(eph->eph_fd, SOL_SOCKET, SO_ERROR, &rc, &rc_len);
        if (rc2)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "Error getting socket error: %d", rc2);
            return rc2;
        }
    }

    return -rc;
}


static raft_net_cb_ctx_t
raft_net_tcp_cb(const struct epoll_handle *eph, uint32_t events, bool from_peer)
{
    static char sink_buf[RAFT_NET_MAX_TCP_RPC_SIZE];
    static struct sockaddr_in from;
    static struct iovec iovs[1] = {
        [0].iov_base = (void *)sink_buf,
        [0].iov_len  = RAFT_NET_MAX_TCP_RPC_SIZE,
    };

    SIMPLE_LOG_MSG(LL_ERROR, "raft_net_tcp_cb()");

    NIOVA_ASSERT(eph && eph->eph_arg);

    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp recv fd=%d, events=%d", eph->eph_fd, events);
    int rc = epoll_handle_rc_get(eph, events);
    if (rc)
    {
        // XXX maybe this should be handled in the poll function?
        SIMPLE_LOG_MSG(LL_NOTIFY, "error received on socket fd=%d, rc=%d", eph->eph_fd, rc);
        return;
    }

    struct raft_net_connection *rnc = eph->eph_arg;
    NIOVA_ASSERT(rnc);

    struct raft_instance *ri = rnc->rnc_ri;

    /* Clear the fd descriptor before doing any other error checks on the
     * sender.
     */
    ssize_t recv_bytes = tcp_socket_recv_fd(eph->eph_fd, iovs, 1, &from, false);

    if (recv_bytes < 0) // return from a general recv error
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "tcp_socket_recv_fd():  %s",
                          strerror(-recv_bytes));
        return;
    }

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "fd=%d rc=%zd",
                      eph->eph_fd,
                      recv_bytes);

    SIMPLE_LOG_MSG(LL_NOTIFY, "from_peer")
    if (from_peer && ri->ri_server_recv_cb)
    {
        ri->ri_server_recv_cb(ri, sink_buf, recv_bytes, &from);
    }
    else if (!from_peer && ri->ri_client_recv_cb)
    {
        ri->ri_client_recv_cb(ri, sink_buf, recv_bytes, &from);
    }
}

static raft_net_cb_ctx_t
raft_net_tcp_recv_client_cb(const struct epoll_handle *eph, uint32_t events)
{
    raft_net_tcp_cb(eph, events, false);
}

static raft_net_cb_ctx_t
raft_net_tcp_recv_server_cb(const struct epoll_handle *eph, uint32_t events)
{
    raft_net_tcp_cb(eph, events, true);
}

static void
raft_net_connection_lookup_csn(struct raft_net_connection *rnc, struct ctl_svc_node **ret)
{
    char *peer = (char *)rnc - offsetof(struct ctl_svc_node_peer, csnp_net_data);
    *ret = (struct ctl_svc_node *)(peer - offsetof(struct ctl_svc_node, csn_peer));
}

static raft_net_cb_ctx_t
raft_net_tcp_connect_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct raft_net_connection *rnc = eph->eph_arg;
    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_tcp_connect_cb(): connecting to %s:%d[%p], eph_events %d", rnc->rnc_tsh.tsh_ipaddr, rnc->rnc_tsh.tsh_port, rnc, eph->eph_events);

    int rc = epoll_handle_rc_get(eph, events);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_tcp_connect_cb(): error, rc=%d", rc);
        tcp_socket_close(&rnc->rnc_tsh);
        return;
    }

    rc = raft_net_tcp_handshake_send(rnc);
    if (rc < 0)
    {
        tcp_socket_close(&rnc->rnc_tsh);
        return;
    }

    // delete epoll handler to re-add with new event type
    // XXX should we add a new wrapper fn for EPOLL_CTL_MOD?
    epoll_handle_del(&rnc->rnc_ri->ri_epoll_mgr, &rnc->rnc_eph);

    struct ctl_svc_node *rp;
    raft_net_connection_lookup_csn(rnc, &rp);
    if (rp->csn_type == CTL_SVC_NODE_TYPE_RAFT_CLIENT)
    {
        epoll_handle_init(&rnc->rnc_eph, rnc->rnc_tsh.tsh_socket, EPOLLIN, raft_net_tcp_recv_client_cb, rnc);
    }
    else
    {
        epoll_handle_init(&rnc->rnc_eph, rnc->rnc_tsh.tsh_socket, EPOLLIN, raft_net_tcp_recv_server_cb, rnc);
    }
    epoll_handle_add(&rnc->rnc_ri->ri_epoll_mgr, &rnc->rnc_eph);

    DBG_CTL_SVC_NODE(LL_NOTIFY, rp, "outgoing connection established");
    rp->csn_peer.csnp_net_data.rnc_status = RNCS_CONNECTED;
}

static void
raft_net_csn_init_socket(struct ctl_svc_node *rp, struct raft_net_connection *rnc)
{
    strncpy(rnc->rnc_tsh.tsh_ipaddr, ctl_svc_node_peer_2_ipaddr(rp), sizeof(rnc->rnc_tsh.tsh_ipaddr));
    rnc->rnc_tsh.tsh_port = ctl_svc_node_peer_2_port(rp);
}

static int
raft_net_tcp_connect(struct ctl_svc_node *rp)
{
    DBG_SIMPLE_CTL_SVC_NODE(LL_NOTIFY, rp, "");
    struct raft_net_connection *rnc = &rp->csn_peer.csnp_net_data;
    int rc = tcp_socket_setup(&rnc->rnc_tsh);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_setup(): %d", rc);
        return rc;
    }

    raft_net_csn_init_socket(rp, rnc);
    rc = tcp_socket_connect(&rnc->rnc_tsh);
    if (rc < 0 && rc != -EINPROGRESS)
    {
        tcp_socket_close(&rnc->rnc_tsh);
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_connect(): %d", rc );
        return rc;
    }

    epoll_handle_init(&rnc->rnc_eph, rnc->rnc_tsh.tsh_socket, EPOLLOUT,
        raft_net_tcp_connect_cb, rnc);
    epoll_handle_add(&rnc->rnc_ri->ri_epoll_mgr, &rnc->rnc_eph);

    if (rc == -EINPROGRESS) {
        rnc->rnc_status = RNCS_CONNECTING;
        SIMPLE_LOG_MSG(LL_NOTIFY, "waiting for connect callback, fd = %d", rnc->rnc_tsh.tsh_socket);
    }
    else
    { // rc = 0
        raft_net_tcp_connect_cb(&rnc->rnc_eph, 0);
    }

    return rc;
}

static raft_net_cb_ctx_t
raft_net_tcp_handshake_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);

    static struct raft_rpc_msg handshake;
    static struct iovec iov = {
        .iov_base = (void *)&handshake,
        .iov_len  = sizeof(handshake),
    };

    NIOVA_ASSERT(eph && eph->eph_arg);

    SIMPLE_LOG_MSG(LL_ERROR, "raft_net_tcp_handshake_cb()");
    struct raft_net_connection *rnc = eph->eph_arg;
    NIOVA_ASSERT(rnc);

    int rc = tcp_socket_recv(&rnc->rnc_tsh, &iov, 1, NULL, 1);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_recv(): %s", strerror(-rc));
        raft_net_tcp_incoming_fini(rnc);

        return;
    }

    if (
        uuid_compare(handshake.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(rnc->rnc_ri)) ||
        handshake.rrm_type != RAFT_RPC_MSG_TYPE_ANY ||
        handshake.rrm_version != 0
    )
    {
        DBG_RAFT_MSG(LL_ERROR, &handshake, "invalid raft handshake");
        raft_net_tcp_incoming_fini(rnc);

        return;
    }

    struct ctl_svc_node *csn = NULL;
    ctl_svc_node_lookup(handshake.rrm_sender_id, &csn);
    if (!csn)
    {
        DBG_RAFT_MSG(LL_ERROR, &handshake, "invalid connection om %s:%d", rnc->rnc_tsh.tsh_ipaddr, rnc->rnc_tsh.tsh_port);
        raft_net_tcp_incoming_fini(rnc);

        return;
    };

    if (csn->csn_peer.csnp_net_data.rnc_status == RNCS_CONNECTED ||
        csn->csn_peer.csnp_net_data.rnc_status == RNCS_CONNECTING)
    {
        // connection already in progress
        DBG_CTL_SVC_NODE(LL_NOTIFY, csn, "incoming connection, but outgoing connection already in progress");
        raft_net_tcp_incoming_fini(rnc);

        return;
    }

    // remove epoll reference to incoming rnc, readd to csn rnc
    epoll_handle_del(&rnc->rnc_ri->ri_epoll_mgr, &rnc->rnc_eph);

    // copy rnc data to csn
    csn->csn_peer.csnp_net_data = *rnc;
    niova_free(rnc);
    rnc = NULL;

    struct raft_net_connection *csn_rnc = &csn->csn_peer.csnp_net_data;
    csn_rnc->rnc_status = RNCS_CONNECTED;

    if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT_CLIENT)
    {
        epoll_handle_init(&csn_rnc->rnc_eph, csn_rnc->rnc_tsh.tsh_socket, EPOLLIN, raft_net_tcp_recv_client_cb, csn_rnc);
    }
    else
    {
        epoll_handle_init(&csn_rnc->rnc_eph, csn_rnc->rnc_tsh.tsh_socket, EPOLLIN, raft_net_tcp_recv_server_cb, csn_rnc);
    }
    epoll_handle_add(&csn_rnc->rnc_ri->ri_epoll_mgr, &csn_rnc->rnc_eph);

    DBG_CTL_SVC_NODE(LL_NOTIFY, csn, "incoming connection established");

    ctl_svc_node_put(csn);
}

static raft_net_cb_ctx_t
raft_net_tcp_listen_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);

    NIOVA_ASSERT(eph && eph->eph_arg);

    struct raft_instance *ri = eph->eph_arg;
    NIOVA_ASSERT(ri);

    // adds epoll handler
    int rc = raft_net_tcp_accept(ri, eph->eph_fd);
    if (rc < 0) {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_tcp_accept(): %d", rc);
    }
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

static init_ctx_t NIOVA_CONSTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
raft_net_init(void)
{
    FUNC_ENTRY(LL_NOTIFY);
    LREG_ROOT_OBJECT_ENTRY_INSTALL(raft_net_info);
    return;
}

struct raft_net_connection *
raft_net_remote_connect(struct raft_instance *ri, struct ctl_svc_node *rp)
{
    NIOVA_ASSERT(ri && rp && ctl_svc_node_is_peer(rp));

    struct raft_net_connection *rnc = &rp->csn_peer.csnp_net_data;
    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_remote_connect() - existing rnc %p", rnc);
    if (rnc->rnc_status == RNCS_CONNECTED)
        return rnc;

    if (rnc->rnc_status == RNCS_NEEDS_SETUP)
        raft_net_connection_setup(ri, rnc);

    // connect is treated like send request
    raft_net_update_last_comm_time(ri, rp->csn_uuid, 1);

    int rc = raft_net_tcp_connect(rp);
    if (rc < 0) {
        return NULL;
    }

    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_remote_connect() - new connection %p", rnc);

    return rnc;
}