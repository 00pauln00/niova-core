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

#include "log.h"
#include "udp.h"
#include "epoll_mgr.h"
#include "crc32.h"
#include "alloc.h"
#include "io.h"
#include "random.h"
#include "ctl_svc.h"
#include "raft.h"

REGISTRY_ENTRY_FILE_GENERATE;

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

int
raft_net_udp_sockets_bind(struct raft_instance *ri)
{
    int rc = 0;

    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX && !rc; i++)
        rc = udp_socket_bind(&ri->ri_ush[i]);

    if (rc)
        raft_net_udp_sockets_close(ri);

    return rc;
}

int
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
    for (enum raft_epoll_handles i = 0; i < RAFT_EPOLL_NUM_HANDLES; i++)
        epoll_handle_del(&ri->ri_epoll_mgr, &ri->ri_epoll_handles[i]);

    return epoll_mgr_close(&ri->ri_epoll_mgr);
}

static int
raft_epoll_setup_udp(struct raft_instance *ri, enum raft_epoll_handles reh)
{
    if (!ri ||
        (reh != RAFT_EPOLL_HANDLE_PEER_UDP &&
         reh != RAFT_EPOLL_HANDLE_CLIENT_UDP))
        return -EINVAL;

    enum raft_udp_listen_sockets ruls =
        (reh == RAFT_EPOLL_HANDLE_PEER_UDP) ?
        RAFT_UDP_LISTEN_SERVER : RAFT_UDP_LISTEN_CLIENT;

    int rc = epoll_handle_init(&ri->ri_epoll_handles[reh],
                               ri->ri_ush[ruls].ush_socket,
                               EPOLLIN, ri->ri_udp_recv_cb, ri);

    return rc ? rc : epoll_handle_add(&ri->ri_epoll_mgr, &
                                      ri->ri_epoll_handles[reh]);
}

int
raft_net_epoll_setup_timerfd(struct raft_instance *ri)
{
    if (!ri ||
        (ri->ri_state != RAFT_STATE_CLIENT && !ri->ri_timer_fd_cb))
        return -EINVAL; // Servers must have specified ri_timer_fd_cb

    else if (!ri->ri_timer_fd_cb)
        return 0;

    int rc =
        epoll_handle_init(&ri->ri_epoll_handles[RAFT_EPOLL_HANDLE_TIMERFD],
                          ri->ri_timer_fd, EPOLLIN, ri->ri_timer_fd_cb,
	                  ri);

    return rc ? rc :
        epoll_handle_add(&ri->ri_epoll_mgr,
                         &ri->ri_epoll_handles[RAFT_EPOLL_HANDLE_TIMERFD]);
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

    /* Next, add the udp sockets.
     */
    for (enum raft_udp_listen_sockets i = RAFT_UDP_LISTEN_MIN;
         i < RAFT_UDP_LISTEN_MAX && !rc; i++)
    {
        rc = raft_epoll_setup_udp(ri, i);
    }

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
    /* Check the ri for the needed the UUID strings.
     */
    if (!ri || !ri->ri_raft_uuid_str ||
        (ri->ri_state != RAFT_STATE_CLIENT && !ri->ri_this_peer_uuid_str))
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
        goto cleanup;

    /* Lookup the raft ctl-svc object.
     */
    rc = ctl_svc_node_lookup_by_string(ri->ri_raft_uuid_str, &ri->ri_csn_raft);
    if (rc)
        goto cleanup;

    DBG_CTL_SVC_NODE(LL_WARN, ri->ri_csn_this_peer, "self");
    DBG_CTL_SVC_NODE(LL_WARN, ri->ri_csn_raft, "raft");

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
            goto cleanup;

        DECLARE_AND_INIT_UUID_STR(uuid_str,
                                  csn_raft->csnr_members[i].csrm_peer);

        DBG_CTL_SVC_NODE(LL_WARN, ri->ri_csn_raft,
                         "raft-peer-%hhu %s", i, uuid_str);

        if (!ctl_svc_node_cmp(ri->ri_csn_this_peer, ri->ri_csn_raft_peers[i]))
            this_peer_found_in_raft_node = true;
    }

    if (!this_peer_found_in_raft_node)
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

    if (ri->ri_state != RAFT_STATE_CLIENT)
        raft_server_instance_shutdown(ri);

    int udp_sockets_close = raft_net_udp_sockets_close(ri);

    raft_net_timerfd_close(ri);

    raft_net_conf_destroy(ri);

    return (udp_sockets_close ? udp_sockets_close :
            (epoll_close_rc ? epoll_close_rc : 0));
}

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode)
{
    if (!ri)
        return -EINVAL;

    ri->ri_state = client_mode ? RAFT_STATE_CLIENT : RAFT_STATE_FOLLOWER;

    int rc = raft_net_conf_init(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_conf_init(): %s", strerror(-rc));
        return rc;
    }

    rc = raft_net_udp_sockets_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_net_udp_sockets_setup(): %s",
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
    {
        rc = raft_server_instance_startup(ri);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_WARN, "raft_instance_startup_server(): %s",
                           strerror(-rc));

            raft_server_instance_shutdown(ri);

            return rc;
        }
    }

    rc = raft_net_epoll_setup(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_epoll_setup(): %s", strerror(-rc));

        raft_net_instance_shutdown(ri);
        return rc;
    }

     /* bind() after adding the socket to the epoll set.
     */
    rc = raft_net_udp_sockets_bind(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "raft_server_udp_sockets_bind(): %s",
                       strerror(-rc));
        raft_net_instance_shutdown(ri);
        return rc;
    }

    return 0;
}

int
raft_net_server_instance_run(const char *raft_uuid_str,
                             const char *this_peer_uuid_str)
{
    if (!raft_uuid_str || !this_peer_uuid_str)
        return -EINVAL;

    struct raft_instance ri = {0};

    ri.ri_raft_uuid_str = raft_uuid_str;
    ri.ri_this_peer_uuid_str = this_peer_uuid_str;
    ri.ri_log_fd = -1;

    int rc = raft_net_instance_startup(&ri, false);
    if (rc)
        return rc;

    rc = raft_server_main_loop(&ri);

    raft_net_instance_shutdown(&ri);

    return rc;
}

void
raft_net_instance_apply_callbacks(struct raft_instance *ri,
                                  void (*udp_recv_cb)(const struct
	                                              epoll_handle *),
                                  void (*timer_fd_cb)(const struct
                                                      epoll_handle *))
{
    NIOVA_ASSERT(ri && udp_recv_cb);

    ri->ri_udp_recv_cb = udp_recv_cb;
    ri->ri_timer_fd_cb = timer_fd_cb;
}
