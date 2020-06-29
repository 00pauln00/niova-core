/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdlib.h>
#include <uuid/uuid.h>

#include "alloc.h"
#include "crc32.h"
#include "log.h"
#include "raft_net.h"
#include "raft_client.h"
#include "ref_tree_proto.h"
#include "registry.h"

typedef raft_client_thread_t void *;

#define RAFT_CLIENT_MAX_INSTANCES 8
#define RAFT_CLIENT_MAX_SUB_APP_INSTANCES 4096 // just a guess at this point..
static size_t raftClientSubAppMax = RAFT_CLIENT_MAX_SUB_APP_INSTANCES;

#define RAFT_CLIENT_TIMERFD_EXPIRE_MS 10U
static unsigned long long raftClientTimerFDExpireMS =
    RAFT_CLIENT_TIMERFD_EXPIRE_MS;

static unsigned long long raftClientRetryTimeoutMS =
    (RAFT_CLIENT_TIMERFD_EXPIRE_MS * 2)

static bool raftClientLeaderIsViable = false;

struct raft_client_instance
{
    struct thread_ctl rci_thr_ctl;
};

static pthread_mutex_t raftClientMutex = PTHREAD_MUTEX_INITIALIZER;
static uint8_t raftClientInstanceMap[RAFT_CLIENT_MAX_INSTANCES];
static struct raft_client_instance raftClientInstances[RAFT_CLIENT_MAX_INSTANCES];

struct raft_client_sub_app_msg_handle
{
    struct raft_client_rpc_msg *rcsamh_pending_rpc; // 1 pndg msg per app
    struct timespec             rcsamh_last_attempt;
};

/**
 * raft_client_sub_app - sub-application handle which is used to track pending
 *    requests to the raft backend.
 * @rcsa_uuid:  sub-app identifier
 * @rcsa_rtentry
 */
struct raft_client_sub_app
{
    struct raft_client_user_id            rcsa_rncui;
    REF_TREE_ENTRY(raft_client_sub_app)   rcsa_rtentry;
    struct raft_client_sub_app_msg_handle rcsa_msgh;
};

static inline int
raft_client_sub_app_cmp(const struct raft_client_sub_app *a,
                        const struct raft_client_sub_app *b)
{
    return raft_client_user_id_cmp(&a->rcsa_rncui, &b->rcsa_rncui);
}

REF_TREE_HEAD(raft_client_sub_app_tree, raft_client_sub_app);
REF_TREE_GENERATE(raft_client_sub_app_tree, raft_client_sub_app, rcsa_rtentry,
                  raft_client_sub_app_cmp);

static struct raft_client_sub_app_tree raftClientSubAppTree;


static struct raft_client_sub_app *
raft_client_sub_app_construct(const struct raft_client_sub_app *in)
{
    if (!in)
        return NULL;

    struct raft_client_sub_app *rsca =
        niova_calloc_can_fail((size_t)1, sizeof(struct raft_client_sub_app));

    if (!rsca)
	return NULL;

    raft_net_client_user_id_copy(&rsca->rcsa_rncui, &in->rcsa_rncui);

    return rsca;
}

static int
raft_client_sub_app_destruct(struct raft_client_sub_app *destroy)
{
    if (!destroy)
	return -EINVAL;

    // Pending RPCs must have been detached
    NIOVA_ASSERT(destroy->rcsa_msgh.rcsamh_pending_rpc == NULL);

    niova_free(destroy);

    return 0;
}

static void
raft_client_sub_app_tree_init(void)
{
    REF_TREE_INIT(&raftClientSubAppTree, raft_client_sub_app_construct,
                  raft_client_sub_app_destruct);
}

static int
raft_client_instance_release(struct raft_client_instance *rci)
{
    int rc = -ENOENT;

    pthread_mutex_lock(&raftClientMutex);

    for (size_t i = 0; i < RAFT_CLIENT_MAX_INSTANCES; i++)
    {
        if (rci == &raftClientInstances[i])
        {
            FATAL_IF((raftClientInstanceMap[i] == 0),
                     "raft client instance is not marked as assigned");

            memset(rci, 0, sizeof(*rci));
            raftClientInstanceMap[i] = 0;

            rc = 0;
            break;
        }
    }

    pthread_mutex_unlock(&raftClientMutex);
    return rc;
}

static struct raft_client_instance *
raft_client_instance_assign(void)
{
    struct raft_client_instance *rci = NULL;

    pthread_mutex_lock(&raftClientMutex);

    for (size_t i = 0; i < RAFT_CLIENT_MAX_INSTANCES; i++)
    {
        if (!raftClientInstanceMap[i])
        {
            raftClientInstanceMap[i] = 1;
            rci = &raftClientInstances[i];
        }
    }

    pthread_mutex_unlock(&raftClientMutex);
    return rci;
}

static void
raf_client_timerfd_settime(struct raft_instance *ri)
{
    raft_net_timerfd_settime(ri, raftClientTimerFDExpireMS);
}

static bool
raft_client_ping_target_is_stale(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    return (!ri->ri_csn_leader ||
            raft_client_server_target_is_stale(ri,
                                               ri->ri_csn_leader->csn_uuid)) ?
        true : false;
}

static void
raft_client_set_leader_viability(bool viable)
{
    raftClientLeaderIsViable = viable;
}

static bool
raft_client_leader_is_viable(void)
{
    return raftClientLeaderIsViable;
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
raft_client_rpc_msg_init(struct raft_instance *ri,
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

    rsc_client_rpc_msg_assign_id(rcrm);

    return 0;
}

static int
raft_client_rpc_ping_init(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm)
{
    return raft_client_rpc_msg_init(ri, rcrm, RAFT_CLIENT_RPC_MSG_TYPE_PING,
                                    0, ri->ri_csn_leader);
}

/**
 * rsc_ping_raft_service - send a 'ping' to the raft leader or another node
 *    if our known raft leader is not responsive.  The ping will reply with
 *    application-specific data for this client instance.
 */
static raft_net_timerfd_cb_ctx_t
raft_client_ping_raft_service(struct raft_instance *ri)
{
    if (!ri || !ri->ri_csn_leader)
        return;

    DBG_SIMPLE_CTL_SVC_NODE(LL_DEBUG, ri->ri_csn_leader, "");

    struct raft_client_rpc_msg rcrm;

    int rc = raft_client_rpc_ping_init(ri, &rcrm);
    FATAL_IF((rc), "rsc_client_rpc_ping_init(): %s", strerror(-rc));

    rc = raft_net_send_client_msg(ri, &rcrm);
    if (rc)
        DBG_RAFT_CLIENT_RPC_LEADER(LL_DEBUG, ri, &rcrm,
                                   "raft_net_send_client_msg() %s",
                                   strerror(-rc));
}

static void
raft_client_set_ping_target(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    if (raft_client_ping_target_is_stale(ri))
    {
        raft_client_set_leader_viability(false);

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
 * raft_client_timerfd_cb - callback is executed by the raft internals,
 *    typically after an expiration of raftClientTimerFDExpireMS.  The raft
 *    client uses this mechanism to reissue lingering RPC requests - those
 *    which are pending and have not yet received a reply.   It may optionally
 *    ping the raft backend service as well.
 */
static raft_net_timerfd_cb_ctx_t
raft_client_timerfd_cb(struct raft_instance *ri)
{
    if (!raft_client_leader_is_viable() || raft_client_should_ping_backend())
    {
        raft_client_set_ping_target(ri);
        raft_client_ping_raft_service(ri);
    }

    if (raft_client_leader_is_viable())
        raft_client_check_pending_requests();

    raft_client_timerfd_settime(ri);
}

static raft_client_thread_t
raft_client_thread(void *arg)
{
    struct thread_ctl *tc = arg;

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");

    struct raft_instance *ri = (struct raft_instance *)thread_ctl_get_arg(tc);
    NIOVA_ASSERT(ri);

    // Startup the raft client instance.
    int rc = raft_net_instance_startup(ri, true);
    FATAL_IF((rc), "raft_net_instance_startup(): %s", strerror(-rc));

    THREAD_LOOP_WITH_CTL(tc)
    {
        raft_client_timerfd_settime(ri);

        rc = epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);
	if (rc == -EINTR)
            rc = 0;

        else if (rc)
            break;
    }

    SIMPLE_LOG_MSG((rc ? LL_WARN : LL_DEBUG), "goodbye (rc=%s)",
                   strerror(-rc));

    return (void *)0;
}

int
raft_client_init(const char *raft_uuid_str, const char *raft_client_uuid_str)
{
    struct raft_instance *ri = raft_net_get_instance();

    if (!raft_uuid_str || !raft_client_uuid_str || !ri)
        return -EINVAL;

    ri->ri_raft_uuid_str = raft_uuid_str;
    ri->ri_this_peer_uuid_str = my_uuid_str;

    struct raft_client_instance *rci = raft_client_instance_assign();
    if (!rci)
        return -ENOSPC;

    raft_net_instance_apply_callbacks(ri, raft_client_timerfd_cb,
                                      raft_client_udp_recv_handler, NULL);

    int rc = thread_create_watched(raft_client_thread, &rci->rci_thr_ctl,
                                   "raft_client", (void *)ri, NULL);

    FATAL_IF(rc, "pthread_create(): %s", strerror(errno));
}
