#include <stdio.h>
#include <sys/timerfd.h>

#include "log.h"
#include "udp.h"
#include "epoll_mgr.h"
#include "common.h"
#include "crc32.h"

#include "random.h"
#include "raft.h"
#include "raft_net.h"
#include "raft_test.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define OPTS "u:r:h"

const char *raft_uuid_str;
const char *my_uuid_str;

static struct random_data rand_data;
static char rand_state_buf[RANDOM_STATE_BUF_LEN];

#define RSC_TIMERFD_EXPIRE_MS 100U
#define RSC_STALE_SERVER_TIME_MS (RSC_TIMERFD_EXPIRE_MS * 3U)

/**
 * rsc_random_init - create a private random generator which is only ever
 *    accessed from this file.  The test application relies on the
 *    repeatability of the numeric sequence, which means random_get() cannot
 *    be used here since it may be called elsewhere in the niova library.
 */
static void
rsc_random_init(void)
{
    if (initstate_r(0, rand_state_buf, RANDOM_STATE_BUF_LEN,
                    &rand_data))
        SIMPLE_LOG_MSG(LL_FATAL, "initstate_r() failed: %s", strerror(errno));
}

#if 0
static unsigned int
rsc_random_get(void)
{
    unsigned int result;

    if (random_r(&rand_data, (int *)&result))
	SIMPLE_LOG_MSG(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
}
#endif

static void
rsc_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r UUID -n UUID\n", argv[0]);

    exit(error);
}

static void
rsc_getopt(int argc, char **argv)
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
            rsc_print_help(0, argv);
            break;
	default:
            rsc_print_help(EINVAL, argv);
            break;
	}
    }

    if (!raft_uuid_str || !my_uuid_str)
        rsc_print_help(EINVAL, argv);
}

static void
rsc_timerfd_settime(struct raft_instance *ri)
{
    struct itimerspec its = {0};

    msec_2_timespec(&its.it_value, RSC_TIMERFD_EXPIRE_MS);

    int rc = timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);

    FATAL_IF((rc), "timerfd_settime(): %s", strerror(errno));
}

static void
rsc_udp_recv_handler(struct raft_instance *ri, const char *recv_buffer,
                     ssize_t recv_bytes, const struct sockaddr_in *from)
{
    if (recv_bytes > RAFT_ENTRY_MAX_DATA_SIZE)
        return;

    const struct raft_client_rpc_msg *rcrm =
        (const struct raft_client_rpc_msg *)recv_buffer;

    struct ctl_svc_node *sender_csn =
        raft_net_verify_sender_server_msg(ri, rcrm->rcrm_sender_id,
                                          rcrm->rcrm_raft_id, from);

    if (!sender_csn)
        return;

    raft_net_update_last_comm_time(ri, rcrm->rcrm_sender_id, false);
}

static bool
rsc_server_target_is_stale(const struct raft_instance *ri,
                           const uuid_t server_uuid)
{
    unsigned long long recency_ms = 0;

    int rc = raft_net_comm_recency(ri, raft_peer_2_idx(ri, server_uuid),
                                   RAFT_COMM_RECENCY_UNACKED_SEND,
                                   &recency_ms);

    return (rc || recency_ms > RSC_STALE_SERVER_TIME_MS) ? true : false;
}

static void
rsc_set_ping_target(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    if (!ri->ri_csn_leader ||
        rsc_server_target_is_stale(ri, ri->ri_csn_leader->csn_uuid))
    {
        raft_peer_t target = raft_net_get_most_recently_responsive_server(ri);

        NIOVA_ASSERT(target <
                     ctl_svc_node_raft_2_num_members(ri->ri_csn_raft));

        /* Raft leader here is really a guess.  If 'target' is not the raft
         * leader then it should reply with the UUID of the raft leader.
         */
        ri->ri_csn_leader = ri->ri_csn_raft_peers[target];
    }
}

static int
rsc_client_rpc_msg_init(struct raft_instance *ri,
                        struct raft_client_rpc_msg *rcrm,
                        enum raft_client_rpc_msg_type msg_type,
                        uint16_t data_size, struct ctl_svc_node *dest_csn)
{
    if (!ri || !ri->ri_csn_raft || !rcrm || !dest_csn)
        return -EINVAL;

    else if (msg_type != RAFT_CLIENT_RPC_MSG_TYPE_PING &&
             msg_type != RAFT_CLIENT_RPC_MSG_TYPE_REQUEST)
        return -EOPNOTSUPP;

    else if (msg_type == RAFT_CLIENT_RPC_MSG_TYPE_REQUEST &&
             (data_size == 0 ||
              (data_size + sizeof(struct raft_client_rpc_msg) >
               RAFT_NET_MAX_RPC_SIZE)))
        return -EMSGSIZE;

    memset(rcrm, 0, sizeof(struct raft_client_rpc_msg));

    rcrm->rcrm_type = msg_type;
    rcrm->rcrm_version = 0;
    rcrm->rcrm_data_size = data_size;

    uuid_copy(rcrm->rcrm_raft_id, ri->ri_csn_raft->csn_uuid);
    uuid_copy(rcrm->rcrm_dest_id, dest_csn->csn_uuid);
    uuid_copy(rcrm->rcrm_sender_id, ri->ri_csn_this_peer->csn_uuid);

    // Generate the msg-id using our UUID as a base.
    uint64_t uuid_int[2];
    niova_uuid_2_uint64(ri->ri_csn_this_peer->csn_uuid, &uuid_int[0],
                        &uuid_int[1]);

    rcrm->rcrm_msg_id = uuid_int[0] ^ uuid_int[1] ^ random_get();

    return 0;
}

static int
rsc_client_rpc_ping_init(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm)
{
    return rsc_client_rpc_msg_init(ri, rcrm, RAFT_CLIENT_RPC_MSG_TYPE_PING,
                                   0, ri->ri_csn_leader);
}

/**
 * rsc_ping_raft_service - send a 'ping' to the raft leader or another node
 *    if our known raft leader is not responsive.  The ping will reply with
 *    application-specific data for this client instance.
 */
static raft_net_timerfd_cb_ctx_t
rsc_ping_raft_service(struct raft_instance *ri)
{
    if (!ri || !ri->ri_csn_leader)
        return;

    DBG_SIMPLE_CTL_SVC_NODE(LL_DEBUG, ri->ri_csn_leader, "");

    struct raft_client_rpc_msg rcrm;

    int rc = rsc_client_rpc_ping_init(ri, &rcrm);
    FATAL_IF((rc), "rsc_client_rpc_ping_init(): %s", strerror(-rc));

    rc = raft_net_send_client_msg(ri, &rcrm);
    if (rc)
    {
        struct sockaddr_in dest;
        struct ctl_svc_node *csn = ri->ri_csn_leader;
        int rc = udp_setup_sockaddr_in(ctl_svc_node_peer_2_ipaddr(csn),
                                       ctl_svc_node_peer_2_client_port(csn),
                                       &dest);

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, &rcrm, &dest,
                            "raft_net_send_client_msg() %s", strerror(-rc));
    }
}

/**
 * rsc_timerfd_cb - callback which is run when the timer_fd expires.
 */
static raft_net_timerfd_cb_ctx_t
rsc_timerfd_cb(struct raft_instance *ri)
{
    rsc_set_ping_target(ri);
    rsc_ping_raft_service(ri);

    // Reset the timer before returning.
    rsc_timerfd_settime(ri);
}

static int
rsc_main_loop(struct raft_instance *ri)
{
    if (!ri || ri->ri_state != RAFT_STATE_CLIENT)
        return -EINVAL;

    int rc = 0;

    do
    {
        rsc_timerfd_settime(ri);

        rc = epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);
        if (rc == -EINTR)
            rc = 0;
    } while (rc >= 0);

    return rc;
}

int
main(int argc, char **argv)
{
    struct raft_instance raft_client_instance = {0};

    rsc_getopt(argc, argv);

    rsc_random_init();

    raft_client_instance.ri_raft_uuid_str = raft_uuid_str;
    raft_client_instance.ri_this_peer_uuid_str = my_uuid_str;

    raft_net_instance_apply_callbacks(&raft_client_instance, rsc_timerfd_cb,
                                      rsc_udp_recv_handler, NULL);

    int rc = raft_net_instance_startup(&raft_client_instance, true);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_instance_startup(): %s",
                       strerror(-rc));
        exit(-rc);
    }

    rc = rsc_main_loop(&raft_client_instance);

    exit(rc);
}
