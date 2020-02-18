#include <stdio.h>
#include <unistd.h>

#include "raft_net.h"
#include "raft_test.h"

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
        log_msg(LL_FATAL, "initstate_r() failed: %s", strerror(errno));
}

static unsigned int
rsc_random_get(void)
{
    unsigned int result;

    if (random_r(&rand_data, (int *)&result))
	log_msg(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
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

    raft_election_timeout_set(&its.it_value);

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
        (const struct raft_rpc_msg *)recv_buffer;

    struct ctl_svc_node *sender_csn =
        raft_net_verify_sender_server_msg(ri, rcrm->rcrm_sender_id,
                                          rcrm->rcrm_raft_id, from);

    if (!sender_csn)
        return;

    raft_net_update_last_comm_time(ri, rcrm->rcrm_sender_id, false);


}

static raft_net_udp_cb_ctx_t
rsc_udp_epoll_cb(const struct epoll_handle *eph)
{
    static char sink_buf[RAFT_ENTRY_SIZE]; // 64kib
    static struct sockaddr_in from;
    static struct iovec iovs[1] = {
        [0].iov_base = (void *)sink_buf,
        [0].iov_len  = RAFT_ENTRY_SIZE,
    };

    NIOVA_ASSERT(eph && eph->eph_arg);

    struct raft_instance *ri = eph->eph_arg;

    /* Clear the fd descriptor before doing any other error checks on the
     * sender.
     */
    ssize_t recv_bytes =
        udp_socket_recv_fd(eph->eph_fd, iovs, 1, &from, false);

    if (recv_bytes < 0) // return from a general recv error
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "udp_socket_recv_fd():  %s",
                       strerror(-recv_bytes));
        return;
    }

    rsc_udp_recv_handler(ri, iovs[0].iov_base, recv_bytes, &from);
}

static void
rsc_set_ping_target(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    if (!ri->ri_csn_leader || rsc_server_target_is_stale(ri->ri_csn_leader))
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

/**
 * rsc_ping_raft_service - send a 'ping' to the raft leader or another node
 *    if our known raft leader is not responsive.  The ping will reply with
 *    application-specific data for this client instance.
 */
static raft_timerfd_cb_ctx_t
rsc_ping_raft_service(struct raft_instance *ri)
{

}

/**
 * rsc_timerfd_cb - callback which is run when the timer_fd expires.
 */
static raft_timerfd_cb_ctx_t
rsc_timerfd_cb(const struct epoll_handle *eph)
{
    struct raft_instance *ri = eph->eph_arg;

    ssize_t rc = io_fd_drain(ri->ri_timer_fd, NULL);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "io_fd_drain(): %zd", rc);
        return;
    }

    rsc_send_ping_to_raft_service(ri);
}

static int
rsc_main_loop(struct raft_instance *ri)
{
    if (!ri || ri->ri_state != RAFT_STATE_CLIENT)
        return -EINVAL;

    do
    {
        rsc_timerfd_settime(ri);

        rc = epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);
        if (rc == -EINTR)
            rc = 0;
    } while (rc > 0);

    return rc;
}

int
main(int argc, char **argv)
{
    struct raft_instance raft_client_instance = {0};

    rsc_getopt(argc, argv);

    rsc_random_init();

    raft_net_instance_apply_callbacks(&raft_client_instance,
                                      rsc_udp_recv_cb, rsc_timerfd_cb);

    int rc = raft_net_instance_startup(&raft_client_instance, true);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_instance_startup(): %s",
                       strerror(-rc));
        exit(-rc);
    }

    rc = rsc_main_loop(&ri);

    exit(rc);
}
