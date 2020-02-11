#include <stdio.h>
#include <unistd.h>

#include "raft_net.h"
#include "raft_test.h"

#define OPTS "u:r:h"

const char *raft_uuid_str;
const char *my_uuid_str;

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

static int
raft_client_loop(struct raft_instance *ri)
{
    if (!ri || ri->ri_state != RAFT_STATE_CLIENT)
        return -EINVAL;

    //First todo, contact the raft leader and gather the seqno and val
    //for my UUID

    // need a cb function which is placed inside of the raft instance
}

static void
rtc_udp_recv_cb(struct raft_instance *ri, const char *recv_buffer,
                ssize_t recv_bytes, const struct sockaddr_in *from)
{

}

static raft_net_udp_cb_ctx_t
rtc_udp_epoll_cb(const struct epoll_handle *eph)
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
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "udp_socket_recv_fd():  %s",
                          strerror(-recv_bytes));
        return;
    }

}


int
main(int argc, char **argv)
{
    struct raft_instance raft_client_instance = {0};

    rsc_getopt(argc, argv);

    raft_net_instance_apply_callbacks(&raft_client_instance,
                                      rtc_udp_recv_cb, rtc_timerfd_fd);

    int rc = raft_net_instance_startup(&raft_client_instance, true);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_instance_startup(): %s",
                       strerror(-rc));
        exit(-rc);
    }

    rc = raft_client_loop(&ri);

    exit(rc);
}
