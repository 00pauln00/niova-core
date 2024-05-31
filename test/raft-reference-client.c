#include <stdio.h>
#include <sys/timerfd.h>

#include "niova_backtrace.h"

#include "log.h"
#include "util_thread.h"
#include "udp.h"
#include "epoll_mgr.h"
#include "common.h"
#include "crc32.h"
#include "binary_hist.h"
#include "random.h"
#include "raft.h"
#include "raft_net.h"
#include "raft_test.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define OPTS "u:r:h"

#define SUCCESSFUL_PING_UNTIL_VIABLE 10

enum raft_client_instance_lreg_values
{
    RAFT_CLIENT_LREG_RAFT_UUID,
    RAFT_CLIENT_LREG_PEER_UUID,
    RAFT_CLIENT_LREG_LEADER_UUID,
    RAFT_CLIENT_LREG_PEER_STATE,
    RAFT_CLIENT_LREG_COMMIT_LATENCY,
    RAFT_CLIENT_LREG_READ_LATENCY,
    RAFT_CLIENT_LREG_MAX,
};

enum raft_client_app_lreg_values
{
    RAFT_CLIENT_APP_LREG_UUID,                   //string
    RAFT_CLIENT_APP_LREG_INITIALIZED,            //bool
    RAFT_CLIENT_APP_LREG_COMMITTED_SEQNO,        //uint64
    RAFT_CLIENT_APP_LREG_COMMITTED_XOR_SUM,      //uint64
    RAFT_CLIENT_APP_LREG_PENDING_SEQNO,          //uint64
    RAFT_CLIENT_APP_LREG_PENDING_VAL,            //uint64
    RAFT_CLIENT_APP_LREG_PENDING_MSG_ID,         //uint64
    RAFT_CLIENT_APP_LREG_LAST_RETRIED_SEQNO,     //uint64
    RAFT_CLIENT_APP_LREG_NUM_WRITE_RETRIES,      //uint64
    RAFT_CLIENT_APP_LREG_LAST_VALIDATED_SEQNO,   //uint64
    RAFT_CLIENT_APP_LREG_LAST_VALIDATED_XOR_SUM, //uint64
    RAFT_CLIENT_APP_LREG_LEADER_ALIVE_CNT,       //int
    RAFT_CLIENT_APP_LREG_LEADER_VIABLE,          //string
    RAFT_CLIENT_APP_LREG_LAST_MSG_RECVD,         //string
    RAFT_CLIENT_APP_LREG_LAST_REQUEST_ACKD,      //string
    RAFT_CLIENT_APP_LREG_MAX,
};

struct rsc_raft_test_info
{
    bool                        rtti_leader_is_viable;
    bool                        rtti_initialized;
    const uint32_t              rtti_random_seed;
    struct timespec             rtti_last_request_sent;
    struct timespec             rtti_last_request_ackd; // by leader
    struct timespec             rtti_last_msg_recvd;
    struct raft_test_values     rrti_committed;
    struct raft_test_values     rrti_last_retried;
    struct raft_test_values     rrti_last_validated;
    size_t                      rrti_num_write_retries;
    // <---- Keep the below members intact ---->
#if defined(__GNUC__) && defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-variable-sized-type-not-at-end"
#endif
    struct raft_client_rpc_msg  rtti_rcrm;
    struct raft_test_data_block rtti_rtdb;
    char                        rtti_payload[RAFT_NET_MAX_RPC_SIZE_POSIX];
#if defined(__GNUC__) && defined(__clang__)
#pragma clang diagnostic pop
#endif
};

/**
 * The raft_client_test maintains only a single instance of
 * rsc_raft_test_info - including the RPC request contents for read and write
 * requests.  Read and write requests are completely serialized.  Pings,
 * however, may run concurrently with read / write requests.
 */
static struct rsc_raft_test_info rRTI;

static util_thread_ctx_reg_int_t
raft_client_test_app_lreg_multi_facet_cb(enum lreg_node_cb_ops,
                                         struct lreg_value *, void *);

LREG_ROOT_ENTRY_GENERATE(raft_client_test_root_entry,
                         LREG_USER_TYPE_RAFT_CLIENT);

LREG_ROOT_ENTRY_GENERATE_OBJECT(raft_client_app,
                                LREG_USER_TYPE_RAFT_CLIENT,
                                RAFT_CLIENT_APP_LREG_MAX,
                                raft_client_test_app_lreg_multi_facet_cb,
                                NULL, LREG_INIT_OPT_NONE);

const char *raft_uuid_str;
const char *my_uuid_str;

static const struct ctl_svc_node *leaderCsn;
static size_t leaderAliveCount;

static struct random_data randData;
static char randStateBuf[RANDOM_STATE_BUF_LEN];

#define RSC_TIMERFD_EXPIRE_MS 1U
#define RSC_STALE_SERVER_TIME_MS (RSC_TIMERFD_EXPIRE_MS * 5U)

#if 0
static struct rsc_raft_test_info *
rsc_get_raft_test_info(void)
{
    return &rRTI;
}
#endif

static void
rsc_set_initialized(void)
{
    NIOVA_ASSERT(!rRTI.rtti_initialized);
    rRTI.rtti_initialized = true;
}

static void
rsc_set_last_validated(const struct raft_test_values *rtv)
{
    if (rtv)
        rRTI.rrti_last_validated = *rtv;
}

static uint64_t
rsc_get_last_validated_seqno(void)
{
    return rRTI.rrti_last_validated.rtv_seqno;
}

static uint64_t
rsc_get_last_validated_xor_sum(void)
{
    return rRTI.rrti_last_validated.rtv_reply_xor_all_values;
}

static bool
rsc_is_initialized(void)
{
    return rRTI.rtti_initialized;
}

static void
rsc_set_leader_viability(bool viable)
{
    rRTI.rtti_leader_is_viable = viable;
}

static bool
rsc_leader_is_viable(void)
{
    return rRTI.rtti_leader_is_viable;
}

static uint64_t
rsc_get_committed_seqno(void)
{
    return rRTI.rrti_committed.rtv_seqno;
}

static uint64_t
rsc_get_committed_xor_sum(void)
{
    return rRTI.rrti_committed.rtv_reply_xor_all_values;
}

static struct timespec *
rsc_get_last_msg_recvd(void)
{
    return &rRTI.rtti_last_msg_recvd;
}

static struct timespec *
rsc_get_last_request_ackd(void)
{
    return &rRTI.rtti_last_request_ackd;
}

static void
rsc_set_last_request_ackd(const struct timespec *ts)
{
    if (ts)
        rRTI.rtti_last_request_ackd = *ts;
}

static struct timespec *
rsc_get_last_request_sent(void)
{
    return &rRTI.rtti_last_request_sent;
}

static struct raft_test_data_block *
rsc_get_app_rtdb(void)
{
    return &rRTI.rtti_rtdb;
}

static struct raft_client_rpc_msg *
rsc_get_app_rcrm(void)
{
    return &rRTI.rtti_rcrm;
}

static uint32_t
rsc_get_random_seed(void)
{
    NIOVA_ASSERT(rRTI.rtti_random_seed);

    return rRTI.rtti_random_seed;
}

/**
 * rsc_get_pending_msg_id - obtain the unique message identifier from the last
 *    RPC issued.
 */
static uint64_t
rsc_get_pending_msg_id(void)
{
    return rRTI.rtti_rcrm.rcrm_msg_id;
}

static void
rsc_clear_pending_msg_id(void)
{
    rRTI.rtti_rcrm.rcrm_msg_id = 0;
}

static int64_t
rsc_get_pending_seqno(void)
{
    if (rsc_get_pending_msg_id())
    {
        struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

        return rtdb->rtdb_values[rtdb->rtdb_num_values - 1].rtv_seqno;
    }

    return -1;
}

static int64_t
rsc_get_pending_value(void)
{
    if (rsc_get_pending_msg_id())
    {
        struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

        return rtdb->rtdb_values[rtdb->rtdb_num_values - 1].rtv_request_value;
    }

    return -1;
}

static void
rsc_update_retry_seqno(void)
{
    if (rsc_get_pending_msg_id())
    {
        struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

        if (rtdb->rtdb_op == RAFT_TEST_DATA_OP_WRITE)
        {
            rRTI.rrti_last_retried =
                rtdb->rtdb_values[rtdb->rtdb_num_values - 1];

            rRTI.rrti_num_write_retries++;
        }
    }
}

static uint64_t
rsc_get_last_retry_seqno(void)
{
    return rRTI.rrti_last_retried.rtv_seqno;
}

static size_t
rsc_get_num_write_retries(void)
{
    return rRTI.rrti_num_write_retries;
}

#if 0
static uint64_t
rsc_get_last_retry_request_value(void)
{
    return rRTI.rrti_last_retried.rtv_request_value;
}
#endif

static unsigned int
rsc_random_get(struct random_data *rand_data)
{
    unsigned int result;

    if (random_r(rand_data, (int *)&result))
        SIMPLE_LOG_MSG(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
}

static void
rsc_init_global_raft_test_info(const struct raft_test_values *rtv)
{
    NIOVA_ASSERT(rtv && rtv->rtv_seqno > 0);
    NIOVA_ASSERT(rsc_get_committed_seqno() == 0);

    rRTI.rrti_committed = *rtv;

    uint64_t xor_all_values = 0;

    /* Fast-forward randData to the correct position.  Note that this brute-
     * force approach was taken after some failed experiments with
     * setstate_r().
     */
    for (uint64_t i = 1; i <= rtv->rtv_seqno; i++)
    {
        uint32_t val = rsc_random_get(&randData);
        xor_all_values ^= val;

        SIMPLE_LOG_MSG(LL_TRACE, "val=%u all=%lu", val, xor_all_values);
    }

    LOG_MSG(LL_NOTIFY, "committed=%ld val=%ld",
            rtv->rtv_seqno, rtv->rtv_reply_xor_all_values);

    NIOVA_ASSERT(xor_all_values == rtv->rtv_reply_xor_all_values);
}

static void
rsc_commit_rtv_to_raft_test_info(const struct raft_test_values *rtv)
{
    NIOVA_ASSERT(rtv);

    if (!rRTI.rrti_committed.rtv_seqno)
        NIOVA_ASSERT(!rRTI.rrti_committed.rtv_reply_xor_all_values);

    FATAL_IF((rtv->rtv_seqno != rRTI.rrti_committed.rtv_seqno + 1),
             "invalid seqno %lu, expected %lu",
             rtv->rtv_seqno, rRTI.rrti_committed.rtv_seqno + 1);

    rRTI.rrti_committed.rtv_seqno++;
    rRTI.rrti_committed.rtv_reply_xor_all_values ^= rtv->rtv_request_value;

    LOG_MSG(LL_NOTIFY, "committed=%ld val=%ld",
            rtv->rtv_seqno, rtv->rtv_reply_xor_all_values);
}

static void
rsc_init_random_seed(const uuid_t self_uuid)
{
    NIOVA_ASSERT(!rRTI.rtti_random_seed);

    CONST_OVERRIDE(uint32_t, rRTI.rtti_random_seed,
                   random_create_seed_from_uuid(self_uuid));

    NIOVA_ASSERT(rRTI.rtti_random_seed);
}

/**
 * rsc_random_init - create a private random generator which is only ever
 *    accessed from this file.  The test application relies on the
 *    repeatability of the numeric sequence, which means random_get() cannot
 *    be used here since it may be called elsewhere in the niova library.
 */
static void
rsc_random_init(struct random_data *rand_data, char *rand_state_buf)
{
    if (initstate_r(rsc_get_random_seed(), rand_state_buf,
                    RANDOM_STATE_BUF_LEN, rand_data))
        SIMPLE_LOG_MSG(LL_FATAL, "initstate_r() failed: %s", strerror(errno));
}

static int
rsc_commit_seqno_validate(const struct raft_test_values *rtv,
                          bool initialize_app)
{
    if (!rtv)
        return -EINVAL;

    else if (!initialize_app && rtv->rtv_seqno > rsc_get_committed_seqno())
        return -ERANGE;

    uint64_t locally_generated_seq = 0;

    // Init to zero per initstate_r man page.
    struct random_data rand_data = {0};
    char rand_state_buf[RANDOM_STATE_BUF_LEN] = {0};

    rsc_random_init(&rand_data, rand_state_buf);

    for (uint64_t i = 1; i <= rtv->rtv_seqno; i++)
    {
        uint32_t tmp = rsc_random_get(&rand_data);
        locally_generated_seq ^= tmp;

        SIMPLE_LOG_MSG(LL_TRACE, "seqno=%lu val=%u all-val[self:svr]=%lu:%lu",
                       i, tmp, locally_generated_seq,
                       rtv->rtv_reply_xor_all_values);
    }

    if (locally_generated_seq != rtv->rtv_reply_xor_all_values)
        return -EILSEQ;

    if (initialize_app)
    {
        rsc_init_global_raft_test_info(rtv);

        rsc_set_initialized();
    }

    rsc_set_last_validated(rtv);

    return 0;
}

static int
rsc_bootstrap_committed_seqno(const struct raft_test_values *rtv)

{
    if (rsc_get_committed_seqno())
        return -EALREADY;

    return rsc_commit_seqno_validate(rtv, true);
}

static raft_net_cb_ctx_int_t
rsc_process_write_reply(const struct raft_client_rpc_msg *rcrm)
{
    if (!rcrm)
        return -EINVAL;

    // This rtdb contains the original request contents
    const struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

    if (rcrm->rcrm_sys_error || rcrm->rcrm_app_error)
    {
        enum log_level log_level =
            (rcrm->rcrm_app_error == -EALREADY ||
             rcrm->rcrm_app_error == -EINPROGRESS) ? LL_NOTIFY : LL_WARN;

        DBG_RAFT_TEST_DATA_BLOCK(log_level, rtdb, "sys=%s, app=%s",
                                 strerror(-rcrm->rcrm_sys_error),
                                 strerror(-rcrm->rcrm_app_error));
        return rcrm->rcrm_app_error == -EALREADY ? 0 : rcrm->rcrm_app_error;
    }

    // Assert that the msg id matches - it should have been checked prior
    NIOVA_ASSERT(rcrm->rcrm_msg_id == rsc_get_pending_msg_id());
    NIOVA_ASSERT(rtdb->rtdb_num_values > 0);
    NIOVA_ASSERT(rtdb->rtdb_num_values <= RAFT_TEST_VALUES_MAX);

    for (uint16_t i = 0; i < rtdb->rtdb_num_values; i++)
        rsc_commit_rtv_to_raft_test_info(&rtdb->rtdb_values[i]);

    return 0;
}

static raft_net_cb_ctx_int_t
rsc_process_read_reply(const struct raft_client_rpc_msg *rcrm)
{
    if (!rcrm)
        return -EINVAL;

    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)rcrm->rcrm_data;

    int16_t app_error = rcrm->rcrm_app_error;

    DBG_RAFT_TEST_DATA_BLOCK((app_error ? LL_WARN : LL_NOTIFY), rtdb,
                             "app-error=%s",
                             app_error ? strerror(-app_error) : "OK");
    if (app_error)
    {
        if (app_error == -ENOENT)
            rsc_set_initialized();

        return app_error;
    }

    int rc = 0;

    if (!rsc_get_committed_seqno()) // Capture current value from raft service
    {
        rc = rsc_bootstrap_committed_seqno(&rtdb->rtdb_values[0]);
        FATAL_IF((rc), "rsc_bootstrap_committed_seqno(): %s", strerror(-rc));
    }
    else
    {
        rc = rsc_commit_seqno_validate(&rtdb->rtdb_values[0], false);
        FATAL_IF((rc), "rsc_commit_seqno_validate(): %s", strerror(-rc));
    }

    return 0;
}

static raft_net_cb_ctx_t
rsc_incorporate_ack_measurement(struct raft_instance *ri,
                                const struct raft_client_rpc_msg *rcrm,
                                const struct sockaddr_in *from,
                                const struct timespec *current_time,
                                enum raft_test_data_op op)
{
    if (!ri || !rcrm || !from || !current_time ||
        (op != RAFT_TEST_DATA_OP_READ && op != RAFT_TEST_DATA_OP_WRITE))
        return;

    rsc_set_last_request_ackd(current_time);

    const long long elapsed_msec =
        (long long)(timespec_2_msec(current_time) -
                    timespec_2_msec(rsc_get_last_request_sent()));

    if (elapsed_msec < 0 || elapsed_msec > (3600 * 1000 * 24))
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_WARN, rcrm, from,
                            "unreasonable elapsed time %lld", elapsed_msec);
    }
    else
    {
        enum raft_instance_hist_types type =
            (op == RAFT_TEST_DATA_OP_READ ?
             RAFT_INSTANCE_HIST_READ_LAT_MSEC :
             RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC);

        struct binary_hist *bh = &ri->ri_rihs[type].rihs_bh;

        binary_hist_incorporate_val(bh, elapsed_msec);
        DBG_RAFT_CLIENT_RPC_SOCK(LL_WARN, rcrm, from, "op=%s elapsed time %lld",
                            raft_test_data_op_2_string(op), elapsed_msec);
    }
}

static raft_net_cb_ctx_t
rsc_udp_recv_handler_process_reply(struct raft_instance *ri,
                                   const struct raft_client_rpc_msg *rcrm,
                                   const struct ctl_svc_node *sender_csn,
                                   const struct sockaddr_in *from)
{
    NIOVA_ASSERT(ri && ri->ri_csn_leader && sender_csn && from);

    if (sender_csn != ri->ri_csn_leader)
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                            "reply is not from leader");
        return;
    }
    else if (rcrm->rcrm_data_size < sizeof(struct raft_test_data_block))
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                                 "invalid reply size %u",
                                 rcrm->rcrm_data_size);
        return;
    }
    else if (!rsc_get_pending_msg_id())
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                            "reply received but no outstanding requests");
        return;
    }
    else if (rcrm->rcrm_msg_id != rsc_get_pending_msg_id())
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                            "wrong msg-id, expected %lx",
                            rsc_get_pending_msg_id());
        return;
    }

    // Verify that we're the intended recipient.
    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)rcrm->rcrm_data;

    if (uuid_compare(rtdb->rtdb_client_uuid, ri->ri_csn_this_peer->csn_uuid))
    {
        char wrong_uuid[UUID_STR_LEN] = {0};

        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                            "wrong rtdb_client_uuid %s", wrong_uuid);
        return;
    }

    // Ensure the operation type is valid.
    if (rtdb->rtdb_op != RAFT_TEST_DATA_OP_READ &&
        rtdb->rtdb_op != RAFT_TEST_DATA_OP_WRITE)
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from, "invalid rtdb_op=%hu",
                            rtdb->rtdb_op);
        return;
    }

    // Verify the size per the operation type.
    const size_t expected_size =
        (sizeof(struct raft_test_data_block) +
         rtdb->rtdb_num_values * sizeof(struct raft_test_values));

    uint16_t expected_num_values =
        rtdb->rtdb_op == RAFT_TEST_DATA_OP_READ ? 1 : 0;

    // Override expected value if an error is present.
    if (rcrm->rcrm_app_error || rcrm->rcrm_sys_error)
        expected_num_values = 0;

    if (rtdb->rtdb_num_values != expected_num_values)
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                            "rtdb %s has invalid rtdb_num_values %u",
                            raft_test_data_op_2_string(rtdb->rtdb_op),
                            rtdb->rtdb_num_values);
        return;
    }
    else if (rcrm->rcrm_data_size != expected_size)
    {
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcrm, from,
                            "incorrect rcrm_data_size %s op, expected %zd",
                            raft_test_data_op_2_string(rtdb->rtdb_op),
                            expected_size);
        return;
    }

    struct timespec ts;
    niova_realtime_coarse_clock(&ts); // Exclude handler exec from measurement

    int rc = rtdb->rtdb_op == RAFT_TEST_DATA_OP_READ ?
        rsc_process_read_reply(rcrm) : rsc_process_write_reply(rcrm);

    if (!rc)
        rsc_incorporate_ack_measurement(ri, rcrm, from, &ts, rtdb->rtdb_op);

    rsc_clear_pending_msg_id();
}

static bool
rsc_server_target_is_stale(const struct raft_instance *ri,
                           const uuid_t server_uuid)
{
    unsigned long long recency_ms = 0;

    int rc = raft_net_comm_recency(ri, raft_peer_2_idx(ri, server_uuid),
                                   RAFT_COMM_RECENCY_UNACKED_SEND,
                                   &recency_ms);

    /* rc of '-EALREADY' means that no msg has been sent OR that a redirect
     * was sent by another peer server (the reciept of which would clear out
     * the 'last_send' timestamp).
     */
    if (rc == -EALREADY)
        return false;

    return (rc || recency_ms > RSC_STALE_SERVER_TIME_MS) ? true : false;
}

static raft_net_cb_ctx_t
rsc_update_leader_from_redirect(struct raft_instance *ri,
                                const struct sockaddr_in *from,
                                const struct raft_client_rpc_msg *rcrm)
{
    if (!ri || !rcrm)
        return;

    raft_peer_t leader_idx = raft_peer_2_idx(ri, rcrm->rcrm_redirect_id);

    DBG_RAFT_CLIENT_RPC_SOCK(LL_DEBUG, rcrm, from,
                        "redirect to new leader idx=%hhu", leader_idx);

    if (leader_idx > CTL_SVC_MAX_RAFT_PEERS)
        return;

    ri->ri_csn_leader = ri->ri_csn_raft_peers[leader_idx];

    if (rsc_server_target_is_stale(ri, rcrm->rcrm_redirect_id))
        timespec_clear(&ri->ri_last_send[leader_idx]); // "unstale" the leader

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");
}

static raft_net_cb_ctx_t
rsc_process_ping_reply(const struct raft_client_rpc_msg *rcrm,
                       const struct ctl_svc_node *sender_csn)
{
    if (!rcrm || !sender_csn)
        return;

    if (sender_csn != leaderCsn)
    {
        leaderCsn = NULL;
        leaderAliveCount = 0;
        rsc_set_leader_viability(false);
    }

    switch (rcrm->rcrm_sys_error)
    {
    case 0:
        leaderAliveCount++;
        if (!leaderCsn)
            leaderCsn = sender_csn;

        if (!rsc_leader_is_viable() &&
            leaderAliveCount > SUCCESSFUL_PING_UNTIL_VIABLE)
            rsc_set_leader_viability(true);
        break;
    case -EINPROGRESS: // fall through
    case -EAGAIN:      // fall through
    case -EBUSY:
        leaderAliveCount = 0;
        rsc_set_leader_viability(false);
        break;
    case -ENOENT: // fall through
    case -ENOSYS:
        leaderCsn = NULL;
        leaderAliveCount = 0;
        rsc_set_leader_viability(false);
    default:
        break;
    }
}

static raft_net_cb_ctx_t
rsc_recv_handler(struct raft_instance *ri, const char *recv_buffer,
                 ssize_t recv_bytes, const struct sockaddr_in *from)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);
    if (!ri || !ri->ri_csn_leader || !recv_buffer || !recv_bytes || !from ||
        recv_bytes > (ssize_t)raft_net_max_rpc_size(ri->ri_store_type))
        return;

    const struct raft_client_rpc_msg *rcrm =
        (const struct raft_client_rpc_msg *)recv_buffer;

//Xxx need a stricter check on the msg contents
    struct ctl_svc_node *sender_csn =
        raft_net_verify_sender_server_msg(ri, rcrm->rcrm_sender_id,
                                          rcrm->rcrm_raft_id, from);
    if (!sender_csn)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "cannot verify sender");
        return;
    }

    DBG_RAFT_CLIENT_RPC_SOCK(
        (rcrm->rcrm_sys_error ? LL_NOTIFY : LL_DEBUG), rcrm, from, "%s",
        rcrm->rcrm_sys_error ?
        raft_net_client_rpc_sys_error_2_string(rcrm->rcrm_sys_error) : "");

    raft_net_update_last_comm_time(ri, rcrm->rcrm_sender_id, false);

    /* Copy the last_recv timestamp taken above.  This is used to track
     * the liveness of the raft cluster.
     */
    int rc = raft_net_comm_get_last_recv(ri, rcrm->rcrm_sender_id,
                                         rsc_get_last_msg_recvd());
    FATAL_IF((rc), "raft_net_comm_get_last_recv(): %s", strerror(-rc));

    if (rcrm->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY)
        rsc_process_ping_reply(rcrm, sender_csn);

    else if (rcrm->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT)
        rsc_update_leader_from_redirect(ri, from, rcrm);

    else if (!rcrm->rcrm_sys_error &&
             rcrm->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_REPLY)
        rsc_udp_recv_handler_process_reply(ri, rcrm, sender_csn, from);
}

static bool
rsc_ping_target_is_stale(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    return (!ri->ri_csn_leader ||
            rsc_server_target_is_stale(ri, ri->ri_csn_leader->csn_uuid)) ?
        true : false;
}

static void
rsc_set_ping_target(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    if (rsc_ping_target_is_stale(ri))
    {
        rsc_set_leader_viability(false);

        raft_peer_t target = raft_net_get_most_recently_responsive_server(ri);

        NIOVA_ASSERT(target <
                     ctl_svc_node_raft_2_num_members(ri->ri_csn_raft));

        /* Raft leader here is really a guess.  If 'target' is not the raft
         * leader then it should reply with the UUID of the raft leader.
         */
        ri->ri_csn_leader = ri->ri_csn_raft_peers[target];
    }
}

static void
rsc_client_rpc_msg_assign_id(struct raft_client_rpc_msg *rcrm)
{
    // Generate the msg-id using our UUID as a base.x
    if (rcrm)
        rcrm->rcrm_msg_id =
            ((uint64_t)rsc_get_random_seed() << 32) | random_get();
}

static raft_net_timerfd_cb_ctx_int_t
rsc_client_rpc_msg_init(struct raft_instance *ri,
                        struct raft_client_rpc_msg *rcrm,
                        enum raft_client_rpc_msg_type msg_type,
                        uint16_t data_size, struct ctl_svc_node *dest_csn)
{
    if (!ri || !ri->ri_csn_raft || !rcrm || !dest_csn)
        return -EINVAL;

    else if (msg_type != RAFT_CLIENT_RPC_MSG_TYPE_PING &&
             msg_type != RAFT_CLIENT_RPC_MSG_TYPE_WRITE &&
             msg_type != RAFT_CLIENT_RPC_MSG_TYPE_READ)
        return -EOPNOTSUPP;

    else if ((msg_type == RAFT_CLIENT_RPC_MSG_TYPE_READ ||
              msg_type == RAFT_CLIENT_RPC_MSG_TYPE_WRITE) &&
             (data_size == 0 ||
              (data_size + sizeof(struct raft_client_rpc_msg) >
               raft_net_max_rpc_size(RAFT_INSTANCE_STORE_POSIX_FLAT_FILE))))
        return -EMSGSIZE;

    memset(rcrm, 0, sizeof(struct raft_client_rpc_msg));

    rcrm->rcrm_type = msg_type;
    rcrm->rcrm_version = 0;
    rcrm->rcrm_data_size = data_size;

    uuid_copy(rcrm->rcrm_raft_id, ri->ri_csn_raft->csn_uuid);
    uuid_copy(rcrm->rcrm_dest_id, dest_csn->csn_uuid);
    uuid_copy(rcrm->rcrm_sender_id, ri->ri_csn_this_peer->csn_uuid);

    rsc_client_rpc_msg_assign_id(rcrm);

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
        DBG_RAFT_CLIENT_RPC_LEADER(LL_DEBUG, ri, &rcrm,
                                   "raft_net_send_client_msg() %s",
                                   strerror(-rc));
}

static void
rsc_timerfd_settime(struct raft_instance *ri)
{
    raft_net_timerfd_settime(ri, RSC_TIMERFD_EXPIRE_MS);
}

static raft_net_timerfd_cb_ctx_t
rsc_setup_read_request(struct raft_instance *ri)
{
    if (!ri)
        return;

    struct raft_client_rpc_msg *rcrm = rsc_get_app_rcrm();
    struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

    rtdb->rtdb_op = RAFT_TEST_DATA_OP_READ;
    rtdb->rtdb_num_values = 0;

    int rc =
        rsc_client_rpc_msg_init(ri, rcrm, RAFT_CLIENT_RPC_MSG_TYPE_READ,
                                sizeof(*rtdb), ri->ri_csn_leader);
    NIOVA_ASSERT(!rc);
}

static raft_net_timerfd_cb_ctx_t
rsc_setup_write_request(struct raft_instance *ri)
{
    if (!ri)
        return;

    struct raft_client_rpc_msg *rcrm = rsc_get_app_rcrm();
    struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

    rtdb->rtdb_op = RAFT_TEST_DATA_OP_WRITE;
    rtdb->rtdb_num_values = 1;

    for (uint16_t i = 0; i < rtdb->rtdb_num_values; i++)
    {
        struct raft_test_values *rtv = &rtdb->rtdb_values[i];

        rtv->rtv_seqno = rsc_get_committed_seqno() + 1;
        rtv->rtv_request_value = rsc_random_get(&randData);

        SIMPLE_LOG_MSG(LL_DEBUG, "seqno=%lu val=%lu", rtv->rtv_seqno,
                       rtv->rtv_request_value);
    }

    const size_t request_size =
        sizeof(*rtdb) + (rtdb->rtdb_num_values *
                         sizeof(struct raft_test_values));

    int rc =
        rsc_client_rpc_msg_init(ri, rcrm, RAFT_CLIENT_RPC_MSG_TYPE_WRITE,
                                request_size, ri->ri_csn_leader);
    NIOVA_ASSERT(!rc);
}

static raft_net_timerfd_cb_ctx_t
rsc_schedule_next_request(struct raft_instance *ri, enum raft_test_data_op op)
{
    if (!ri ||
        (op != RAFT_TEST_DATA_OP_READ && op != RAFT_TEST_DATA_OP_WRITE))
        return;

    if (op == RAFT_TEST_DATA_OP_WRITE)
        NIOVA_ASSERT(rsc_is_initialized());

    struct raft_client_rpc_msg *rcrm = rsc_get_app_rcrm();
    struct raft_test_data_block *rtdb = rsc_get_app_rtdb();

    uuid_copy(rtdb->rtdb_client_uuid, ri->ri_csn_this_peer->csn_uuid);

    const struct raft_test_values *possible_pending_rtv =
        &rtdb->rtdb_values[0];

    bool msg_retry = false;

    // If there's a pending write, resend it
    if (rtdb->rtdb_op == RAFT_TEST_DATA_OP_WRITE &&
        possible_pending_rtv->rtv_seqno > rsc_get_committed_seqno())
    {
        NIOVA_ASSERT(rsc_is_initialized());
        NIOVA_ASSERT(possible_pending_rtv->rtv_seqno ==
                     (rsc_get_committed_seqno() + 1));

        rsc_update_retry_seqno();

        const uint64_t prev_msg_id = rsc_get_pending_msg_id();
        rsc_client_rpc_msg_assign_id(rcrm);

        DBG_RAFT_CLIENT_RPC_LEADER(
            LL_NOTIFY, ri, rcrm,
            "resend write request (pending-seqno=%lu) prev_msg_id=%lx",
            possible_pending_rtv->rtv_seqno, prev_msg_id);

        msg_retry = true;
    }
    else if (op == RAFT_TEST_DATA_OP_WRITE)
    {
        rsc_setup_write_request(ri);
    }
    else
    {
        rsc_setup_read_request(ri);
    }

    if (!msg_retry) // Take a timestamp for non-retried msgs
        niova_realtime_coarse_clock(rsc_get_last_request_sent());

    int rc = raft_net_send_client_msg(ri, rcrm);
    if (rc)
        DBG_RAFT_CLIENT_RPC_LEADER(LL_NOTIFY, ri, rcrm,
                                   "raft_net_send_client_msg() %s",
                                   strerror(-rc));
}

/**
 * rsc_timerfd_cb - callback which is run when the timer_fd expires.
 */
static raft_net_timerfd_cb_ctx_t
rsc_timerfd_cb(struct raft_instance *ri)
{
    static size_t exec_cnt;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "rsc_leader_is_viable(): %s",
                      rsc_leader_is_viable() ? "yes" : "no");

    if (!rsc_leader_is_viable() || !(exec_cnt % 3))
    {
        rsc_set_ping_target(ri);
        rsc_ping_raft_service(ri);
    }
    else
    {
        if (rsc_is_initialized())
        {
            if (!rsc_get_committed_seqno())
            {
                rsc_schedule_next_request(ri, RAFT_TEST_DATA_OP_WRITE);
            }
            else
            {
                if (!(exec_cnt % 31))
                    rsc_schedule_next_request(ri, RAFT_TEST_DATA_OP_READ);
                else
                    rsc_schedule_next_request(ri, RAFT_TEST_DATA_OP_WRITE);
            }
        }
        else
        {
            rsc_schedule_next_request(ri, RAFT_TEST_DATA_OP_READ);
        }
    }

    // Reset the timer before returning.
    rsc_timerfd_settime(ri);

    exec_cnt++;
}

static void
rsc_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r <UUID> -u <UUID>\n", argv[0]);

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
static util_thread_ctx_reg_int_t
raft_client_test_instance_hist_lreg_multi_facet_handler(
    enum lreg_node_cb_ops op,
    struct raft_instance_hist_stats *rihs,
    struct lreg_value *lv)
{
    if (!lv || !rihs)
        return -EINVAL;

    int hsz = binary_hist_size(&rihs->rihs_bh);
    if (hsz < 0)
        return hsz;

    if (lv->lrv_value_idx_in >= (uint32_t)hsz)
        return -EINVAL;

    else if (op == LREG_NODE_CB_OP_WRITE_VAL)
        return -EPERM;

    else if (op != LREG_NODE_CB_OP_READ_VAL)
        return -EOPNOTSUPP;

    snprintf(lv->lrv_key_string, LREG_VALUE_STRING_MAX, "%lld",
             binary_hist_lower_bucket_range(&rihs->rihs_bh,
                                            lv->lrv_value_idx_in));

    LREG_VALUE_TO_OUT_SIGNED_INT(lv) =
        binary_hist_get_cnt(&rihs->rihs_bh, lv->lrv_value_idx_in);

    lv->get.lrv_value_type_out = LREG_VAL_TYPE_UNSIGNED_VAL;

    return 0;
}

static util_thread_ctx_reg_int_t
raft_client_test_instance_hist_lreg_cb(enum lreg_node_cb_ops op,
                                       struct lreg_node *lrn,
                                       struct lreg_value *lv)
{
    struct raft_instance_hist_stats *rihs = lrn->lrn_cb_arg;

    if (lv)
        lv->get.lrv_num_keys_out = binary_hist_size(&rihs->rihs_bh);

    int rc = 0;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;

        lreg_value_fill_key_and_type(
            lv, raft_instance_hist_stat_2_name(rihs->rihs_type),
            LREG_VAL_TYPE_OBJECT);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        if (!lv)
            return -EINVAL;

        rc = raft_client_test_instance_hist_lreg_multi_facet_handler(op, rihs,
                                                                     lv);
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: // fall through
    case LREG_NODE_CB_OP_DESTROY_NODE: // fall through
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE:
        break;

    default:
        return -ENOENT;
    }

    return rc;
}

static util_thread_ctx_reg_int_t
raft_client_test_instance_lreg_multi_facet_cb(enum lreg_node_cb_ops op,
                                              const struct raft_instance *ri,
                                              struct lreg_value *lv)
{
    if (!lv || !ri)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= RAFT_CLIENT_LREG_MAX)
        return -ERANGE;

    else if (op == LREG_NODE_CB_OP_WRITE_VAL)
        return -EPERM;

    else if (op != LREG_NODE_CB_OP_READ_VAL)
        return -EOPNOTSUPP;

    switch (lv->lrv_value_idx_in)
    {
    case RAFT_CLIENT_LREG_RAFT_UUID:
        lreg_value_fill_string(lv, "raft-uuid", ri->ri_raft_uuid_str);
        break;
    case RAFT_CLIENT_LREG_PEER_UUID:
        lreg_value_fill_string(lv, "client-uuid", ri->ri_this_peer_uuid_str);
        break;
    case RAFT_CLIENT_LREG_LEADER_UUID:
        if (ri->ri_csn_leader)
            lreg_value_fill_string_uuid(lv, "leader-uuid",
                                        ri->ri_csn_leader->csn_uuid);
        else
            lreg_value_fill_string(lv, "leader-uuid", NULL);
        break;
    case RAFT_CLIENT_LREG_PEER_STATE:
        lreg_value_fill_string(lv, "state",
                               raft_server_state_to_string(ri->ri_state));
        break;
    case RAFT_CLIENT_LREG_COMMIT_LATENCY:
        lreg_value_fill_histogram(lv, "commit-latency-msec",
                                  RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC);
        break;
    case RAFT_CLIENT_LREG_READ_LATENCY:
        lreg_value_fill_histogram(lv, "read-latency-msec",
                                  RAFT_INSTANCE_HIST_READ_LAT_MSEC);
        break;
    default:
        break;
    }

    return 0;
}

static util_thread_ctx_reg_int_t
raft_client_test_instance_lreg_cb(enum lreg_node_cb_ops op,
                                  struct lreg_node *lrn,
                                  struct lreg_value *lv)
{
    const struct raft_instance *ri = lrn->lrn_cb_arg;
    if (!ri)
        return -EINVAL;

    if (lv)
        lv->get.lrv_num_keys_out = RAFT_CLIENT_LREG_MAX;

    int rc = 0;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;
        strncpy(lv->lrv_key_string, "raft_client_instance",
                LREG_VALUE_STRING_MAX);
        strncpy(LREG_VALUE_TO_OUT_STR(lv), ri->ri_this_peer_uuid_str,
                LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL: // fall through
    case LREG_NODE_CB_OP_WRITE_VAL:
        rc = lv ?
            raft_client_test_instance_lreg_multi_facet_cb(op, ri, lv) :
            -EINVAL;
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: // fall through
    case LREG_NODE_CB_OP_DESTROY_NODE: // fall through
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE:
        break;

    default:
        rc = -ENOENT;
        break;
    }

    return rc;
}


static util_thread_ctx_reg_int_t
raft_client_test_app_lreg_multi_facet_cb(enum lreg_node_cb_ops op,
                                         struct lreg_value *lv, void *arg)
{
    if (!lv || arg /* arg is not used here */)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= RAFT_CLIENT_APP_LREG_MAX)
        return -ERANGE;

    else if (op == LREG_NODE_CB_OP_WRITE_VAL)
        return -EPERM;

    else if (op != LREG_NODE_CB_OP_READ_VAL)
        return -EOPNOTSUPP;

    switch (lv->lrv_value_idx_in)
    {
    case RAFT_CLIENT_APP_LREG_UUID:
        lreg_value_fill_string(lv, "app-uuid", my_uuid_str);
        break;
    case RAFT_CLIENT_APP_LREG_INITIALIZED:
        lreg_value_fill_bool(lv, "initialized", rsc_is_initialized());
        break;
    case RAFT_CLIENT_APP_LREG_COMMITTED_SEQNO:
        lreg_value_fill_unsigned(lv, "committed-seqno",
                                 rsc_get_committed_seqno());
        break;
    case RAFT_CLIENT_APP_LREG_COMMITTED_XOR_SUM:
        lreg_value_fill_unsigned(lv, "committed-xor-sum",
                                 rsc_get_committed_xor_sum());
        break;
    case RAFT_CLIENT_APP_LREG_PENDING_SEQNO:
        lreg_value_fill_signed(lv, "pending-seqno", rsc_get_pending_seqno());
        break;
    case RAFT_CLIENT_APP_LREG_PENDING_VAL:
        lreg_value_fill_signed(lv, "pending-value", rsc_get_pending_value());
        break;
    case RAFT_CLIENT_APP_LREG_PENDING_MSG_ID:
        lreg_value_fill_unsigned(lv, "pending-msg-id",
                                 rsc_get_pending_msg_id());
        break;
    case RAFT_CLIENT_APP_LREG_LAST_RETRIED_SEQNO:
        lreg_value_fill_unsigned(lv, "last-retried-seqno",
                                 rsc_get_last_retry_seqno());
        break;
    case RAFT_CLIENT_APP_LREG_NUM_WRITE_RETRIES:
        lreg_value_fill_unsigned(lv, "num-write-retries",
                                 rsc_get_num_write_retries());
        break;
    case RAFT_CLIENT_APP_LREG_LAST_VALIDATED_SEQNO:
        lreg_value_fill_unsigned(lv, "last-validated-seqno",
                                 rsc_get_last_validated_seqno());
        break;
    case RAFT_CLIENT_APP_LREG_LAST_VALIDATED_XOR_SUM:
        lreg_value_fill_unsigned(lv, "last-validated-xor-sum",
                                 rsc_get_last_validated_xor_sum());
        break;
    case RAFT_CLIENT_APP_LREG_LEADER_ALIVE_CNT:
        lreg_value_fill_unsigned(lv, "leader-alive-cnt",
                                 leaderAliveCount);
        break;
    case RAFT_CLIENT_APP_LREG_LEADER_VIABLE:
        lreg_value_fill_bool(lv, "leader-is-viable", rsc_leader_is_viable());
        break;
    case RAFT_CLIENT_APP_LREG_LAST_MSG_RECVD:
        lreg_value_fill_string_time(lv, "last-msg-recvd",
                                    rsc_get_last_msg_recvd()->tv_sec);
        break;
    case RAFT_CLIENT_APP_LREG_LAST_REQUEST_ACKD:
        lreg_value_fill_string_time(lv, "last-request-ack",
                                    rsc_get_last_request_ackd()->tv_sec);
        break;
    default:
        break;
    }

    return 0;
}

static int
raft_client_test_lreg_init(struct raft_instance *ri)
{
    LREG_ROOT_ENTRY_INSTALL(raft_client_test_root_entry);

    lreg_node_init(&ri->ri_lreg, LREG_USER_TYPE_RAFT_CLIENT,
                   raft_client_test_instance_lreg_cb, ri, LREG_INIT_OPT_NONE);

    int rc = lreg_node_install(
        &ri->ri_lreg, LREG_ROOT_ENTRY_PTR(raft_client_test_root_entry));

    if (rc)
        return rc;

    for (enum raft_instance_hist_types i = RAFT_INSTANCE_HIST_MIN;
         i < RAFT_INSTANCE_HIST_MAX; i++)
    {
        lreg_node_init(&ri->ri_rihs[i].rihs_lrn, i,
                       raft_client_test_instance_hist_lreg_cb,
                       (void *)&ri->ri_rihs[i],
                       LREG_INIT_OPT_IGNORE_NUM_VAL_ZERO);

        rc = lreg_node_install(&ri->ri_rihs[i].rihs_lrn, &ri->ri_lreg);
        if (rc)
            return rc;
    }

    LREG_ROOT_OBJECT_ENTRY_INSTALL(raft_client_app);

    return rc;
}

int
main(int argc, char **argv)
{
    struct raft_instance *ri = raft_net_get_instance();

    rsc_getopt(argc, argv);

    ri->ri_raft_uuid_str = raft_uuid_str;
    ri->ri_this_peer_uuid_str = my_uuid_str;
    ri->ri_store_type = RAFT_INSTANCE_STORE_POSIX_FLAT_FILE;

    raft_net_instance_apply_callbacks(ri, rsc_timerfd_cb,
                                      rsc_recv_handler,
                                      rsc_recv_handler);

    int rc = raft_net_instance_startup(ri, true);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_instance_startup(): %s",
                       strerror(-rc));
        exit(-rc);
    }

    rc = raft_client_test_lreg_init(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_client_test_lreg_init(): %s",
                       strerror(-rc));
        exit(-rc);
    }

    rsc_init_random_seed(ri->ri_csn_this_peer->csn_uuid);

    rsc_random_init(&randData, randStateBuf);

    rc = rsc_main_loop(ri);

    exit(rc);
}
