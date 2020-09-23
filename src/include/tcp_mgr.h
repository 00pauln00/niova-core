/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Kit Westneat <kit@niova.io> 2020
 */

#ifndef __NIOVA_TCP_MGR_H_
#define __NIOVA_TCP_MGR_H_ 1

#include "epoll_mgr.h"
#include "tcp.h"

struct tcp_mgr_connection;


typedef int (*tcp_mgr_ref_cb_t)(struct tcp_mgr_connection *tmc,
                                uint32_t do_put); //XXX enum do_put
typedef int (*tcp_mgr_recv_cb_t)(struct tcp_mgr_connection *tmc, void *data);
typedef struct tcp_mgr_connection *(*tcp_mgr_handshake_cb_t)(void *data,
                                                             int fd,
                                                             void *handshake,
                                                             size_t size);
typedef int (*tcp_mgr_handshake_fill_t)(void *data, void *handshake,
                                        size_t size);


struct tcp_mgr_instance
{
    struct tcp_socket_handle tmi_listen_socket;
    struct epoll_mgr        *tmi_epoll_mgr;
    struct epoll_handle      tmi_listen_eph;
    void                    *tmi_data;

    tcp_mgr_ref_cb_t         tmi_connection_ref_cb;
    tcp_mgr_recv_cb_t        tmi_recv_cb;
    tcp_mgr_handshake_cb_t   tmi_handshake_cb;
    tcp_mgr_handshake_fill_t tmi_handshake_fill;
    size_t                   tmi_handshake_size;
};

enum tcp_mgr_connection_status
{
    TMCS_NEEDS_SETUP,
    TMCS_DISCONNECTING,
    TMCS_DISCONNECTED,
    TMCS_CONNECTING,
    TMCS_CONNECTED,
};

struct tcp_mgr_connection
{
    pthread_mutex_t                tmc_status_mutex;
    enum tcp_mgr_connection_status tmc_status;
    struct tcp_socket_handle       tmc_tsh;
    struct epoll_handle            tmc_eph;
    struct tcp_mgr_instance       *tmc_tmi;
};

#define DBG_TCP_MGR_CXN(log_level, tmc, fmt, ...)                    \
{                                                                    \
   SIMPLE_LOG_MSG(log_level, "tmc[%p]: %s:%d " fmt, (tmc),           \
                 (tmc)->tmc_tsh.tsh_ipaddr, (tmc)->tmc_tsh.tsh_port, \
                 ##__VA_ARGS__);                                     \
}

void
tcp_mgr_setup(struct tcp_mgr_instance *tmi, void *data,
              tcp_mgr_ref_cb_t connection_ref_cb,
              tcp_mgr_recv_cb_t recv_cb,
              tcp_mgr_handshake_cb_t handshake_cb,
              tcp_mgr_handshake_fill_t handshake_fill,
              size_t handshake_size);

int
tcp_mgr_sockets_close(struct tcp_mgr_instance *tmi);

int
tcp_mgr_sockets_setup(struct tcp_mgr_instance *tmi, const char *ipaddr,
                      int port);

int
tcp_mgr_sockets_bind(struct tcp_mgr_instance *tmi);

int
tcp_mgr_epoll_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epoll_mgr);

void
tcp_mgr_connection_close(struct tcp_mgr_connection *tmc);
#endif
