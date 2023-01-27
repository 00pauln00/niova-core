/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdlib.h>
#include <uuid/uuid.h>

#include <rocksdb/c.h>

#include "alloc.h"
#include "crc32.h"
#include "log.h"
#include "pumice_db.h"
#include "pumice_db_net.h"
#include "raft.h"
#include "raft_net.h"
#include "raft_server_backend_rocksdb.h"
#include "registry.h"
#include "ref_tree_proto.h"
#include "fault_inject.h"

LREG_ROOT_ENTRY_GENERATE(pumicedb_entry, LREG_USER_TYPE_RAFT);
REGISTRY_ENTRY_FILE_GENERATE;

#define PMDB_COLUMN_FAMILY_NAME "pumiceDB_private"

#define PMDB_SNAPSHOT_MAX_OPEN_TIME_SEC 60

static const struct PmdbAPI *pmdbApi;
static void *pmdb_user_data = NULL;

static struct raft_server_rocksdb_cf_table pmdbCFT = {0};

struct pmdb_range_read_req
{
    uint64_t                            prrq_seq; // Must be the first memb!
    const rocksdb_snapshot_t*           prrq_snap;
    rocksdb_readoptions_t*              prrq_roptions;
    int64_t                             prrq_term;
    struct timespec                     prrq_snap_atime; // snapshot access time
    REF_TREE_ENTRY(pmdb_range_read_req) prrq_rtentry;
    CIRCLEQ_ENTRY(pmdb_range_read_req)  prrq_lentry;
};

CIRCLEQ_HEAD(pmdb_range_read_req_queue, pmdb_range_read_req);

static int
pmdb_range_read_req_cmp(const struct pmdb_range_read_req *a,
                        const struct pmdb_range_read_req *b)
{
    return a->prrq_seq == b->prrq_seq ? 0 : 1;
}

REF_TREE_HEAD(pmdb_range_read_req_tree, pmdb_range_read_req);
REF_TREE_GENERATE(pmdb_range_read_req_tree, pmdb_range_read_req, prrq_rtentry,
                  pmdb_range_read_req_cmp);

static struct pmdb_range_read_req_tree pmdb_range_read_req;
static int64_t pmdb_range_read_tree_term = -1; // All entries in the range read
                                               // should be from same term value.

static int64_t pmdb_current_term = -1;

static struct pmdb_range_read_req_queue prrq_queue;

struct pmdb_cowr_sub_app
{
    struct raft_net_client_user_id    pcwsa_rncui; // Must be the first memb!
    uuid_t                            pcwsa_client_uuid; // client UUID
    int64_t                           pcwsa_current_term;
    REF_TREE_ENTRY(pmdb_cowr_sub_app) pcwsa_rtentry;
};

static int
pmdb_cowr_sub_app_cmp(const struct pmdb_cowr_sub_app *a,
                      const struct pmdb_cowr_sub_app *b)
{
    return raft_net_client_user_id_cmp(&a->pcwsa_rncui,
                                       &b->pcwsa_rncui);
}

REF_TREE_HEAD(pmdb_cowr_sub_app_tree, pmdb_cowr_sub_app);
REF_TREE_GENERATE(pmdb_cowr_sub_app_tree, pmdb_cowr_sub_app, pcwsa_rtentry,
                  pmdb_cowr_sub_app_cmp);

static struct pmdb_cowr_sub_app_tree pmdb_cowr_sub_apps;
static int64_t pmdb_cowr_tree_term = -1;  // All the entries in cowr RB tree should
                                     // be from same term.

struct pmdb_obj_extras_v0
{
    uint64_t pmdb_oextra_commit_term;
    uuid_t   pmdb_oextra_commit_uuid; // UUID of leader at the time
};

/**
 * pmdb_object - object which is stored in the value contents of a RocksDB KV.
 *    The object's role is to store pending write request state for the app
 *    request.  The contents serve to detect requests which are being retried
 *    or have already been committed, as well as containing the info necessary
 *    for formulating and directing a reply.
 * @pmdb_obj_crc:  checksum for the object
 * @pmdb_obj_version:  versioner for the object (currently not used)
 * @pmdb_obj_commit_seqno:  the pumicedb sequence number of this request.
 *   PumiceDB stipulates that each "application" (as determined by the contents
 *   pmdb_obj_rncui) may have only a single pending request.  Values less than
 *   pmdb_obj_commit_seqno have already been committed into raft and have had
 *   contents 'splayed' into RocksDB.  A value equal to
 *   (pmdb_obj_commit_seqno + 1) will cause a write to be logged into Raft if
 *   the term value, pmdb_obj_pending_term, is less than the current term;  a
 *   term value equal to the current term signifies that the request
 *   @(pmdb_obj_commit_seqno + 1) has already been logged into raft but not
 *   committed.  A value greater than (pmdb_obj_commit_seqno + 1) is considered
 *   invalid and signifies that the application holds an incorrect
 *   last-committed value.
 * @pmdb_obj_pending_term:  Term in which a pending write was written into
 *   raft.  This is used to detect stale write requests (ie ones which were
 *   accepted into the local raft log but were not committed due to a leader
 *   change).
 * @pmdb_obj_client_uuid:  Client instance which issued the RPC request.  This
 *   client may be a proxy for more than one application user id (rncui).
 * @pmdb_obj_rncui:  User / application identifier.  This is a rich structure
 *   which may encode several levels of nested identifiers.
 * @pmdb_obj_msg_id:  The RPC identifier used by the client's RPC.
 */
struct pmdb_object
{
    crc32_t                        pmdb_obj_crc;
    version_t                      pmdb_obj_version;
    int64_t                        pmdb_obj_commit_seqno;
    int64_t                        pmdb_obj_pending_term;
    uuid_t                         pmdb_obj_client_uuid;
    struct raft_net_client_user_id pmdb_obj_rncui;
    int64_t                        pmdb_obj_msg_id;
    union
    {
        struct pmdb_obj_extras_v0 v0;
    };
};

struct pmdb_apply_handle
{
    const struct raft_net_client_user_id *pah_rncui;
    struct raft_net_sm_write_supplements *pah_ws;
};

#define PMDB_OBJ_DEBUG(log_level, pmdbo, fmt, ...)                          \
do {                                                                        \
    DEBUG_BLOCK(log_level) {                                                \
        char __uuid_str[UUID_STR_LEN];                                  \
        uuid_unparse(                                                   \
            RAFT_NET_CLIENT_USER_ID_2_UUID(&(pmdbo)->pmdb_obj_rncui, 0, 0), \
            __uuid_str);                                                    \
        LOG_MSG(log_level,                                                  \
            "%s.%lx.%lx.%lx.%lx v=%d crc=%x cs=%ld pt=%ld msg-id=%lx "      \
            fmt,                                                            \
            __uuid_str,                                                     \
            RAFT_NET_CLIENT_USER_ID_2_UINT64(&(pmdbo)->pmdb_obj_rncui,      \
                                             0, 2),                         \
            RAFT_NET_CLIENT_USER_ID_2_UINT64(&(pmdbo)->pmdb_obj_rncui,      \
                                             0, 3),                         \
            RAFT_NET_CLIENT_USER_ID_2_UINT64(&(pmdbo)->pmdb_obj_rncui,      \
                                             0, 4),                         \
            RAFT_NET_CLIENT_USER_ID_2_UINT64(&(pmdbo)->pmdb_obj_rncui,      \
                                             0, 5),                         \
            (pmdbo)->pmdb_obj_version,                                      \
            (pmdbo)->pmdb_obj_crc,                                          \
            (pmdbo)->pmdb_obj_commit_seqno,                                 \
            (pmdbo)->pmdb_obj_pending_term,                                 \
            (pmdbo)->pmdb_obj_msg_id,                                       \
            ##__VA_ARGS__);                                                 \
    }                                                                   \
} while (0)

#define PMDB_STR_DEBUG(log_level, pmdb_rncui, fmt, ...)                \
do {                                                                \
    DEBUG_BLOCK(log_level) {                                                \
        char __uuid_str[UUID_STR_LEN];                                  \
        uuid_unparse(RAFT_NET_CLIENT_USER_ID_2_UUID(pmdb_rncui, 0, 0), \
                     __uuid_str);                                      \
        LOG_MSG(log_level, "%s.%lx.%lx: "fmt,                          \
                __uuid_str,                                            \
            RAFT_NET_CLIENT_USER_ID_2_UINT64(pmdb_rncui, 0, 2),        \
            RAFT_NET_CLIENT_USER_ID_2_UINT64(pmdb_rncui, 0, 3),        \
            ##__VA_ARGS__);                                            \
    }                                                                  \
} while (0)

static void
pmdb_obj_crc_calc(struct pmdb_object *obj)
{
    const size_t offset =
        offsetof(struct pmdb_object, pmdb_obj_crc) + sizeof(crc32_t);

    const unsigned char *buf = (const unsigned char *)obj + offset;
    const int crc_len = sizeof(struct pmdb_object) - offset;
    NIOVA_ASSERT(crc_len >= 0);

    obj->pmdb_obj_crc = crc_pcl(buf, crc_len, 0);
}

static void
pmdb_object_init(struct pmdb_object *pmdb_obj, version_t version,
                 const struct raft_net_client_user_id *pmdbrm_user_id)
{
    NIOVA_ASSERT(pmdb_obj && pmdbrm_user_id);

    memset(pmdb_obj, 0, sizeof(*pmdb_obj));

    pmdb_obj->pmdb_obj_version = version;
    pmdb_obj->pmdb_obj_commit_seqno = ID_ANY_64bit;
    pmdb_obj->pmdb_obj_pending_term = ID_ANY_64bit;

    raft_net_client_user_id_copy(&pmdb_obj->pmdb_obj_rncui, pmdbrm_user_id);

    PMDB_OBJ_DEBUG(LL_DEBUG, pmdb_obj, "");
}

static void
pmdb_object_net_init(struct pmdb_object *pmdb_obj,
                     const uuid_t client_uuid,
                     const int64_t msg_id)
{
    NIOVA_ASSERT(pmdb_obj);

    uuid_copy(pmdb_obj->pmdb_obj_client_uuid, client_uuid);

    pmdb_obj->pmdb_obj_msg_id = msg_id;

    PMDB_OBJ_DEBUG(LL_DEBUG, pmdb_obj, "");
}

static rocksdb_t *
pmdb_get_rocksdb_instance(void)
{
    rocksdb_t *db = raft_server_get_rocksdb_instance(raft_net_get_instance());
    NIOVA_ASSERT(db);

    return db;
}

rocksdb_column_family_handle_t *
PmdbCfHandleLookup(const char *cf_name)
{
    if (cf_name)
    {
        for (size_t i = 0; i < pmdbCFT.rsrcfe_num_cf; i++)
            if (!strncmp(cf_name, pmdbCFT.rsrcfe_cf_names[i],
                         RAFT_ROCKSDB_MAX_CF_NAME_LEN))
                return pmdbCFT.rsrcfe_cf_handles[i];
    }

    return NULL;
}

static rocksdb_column_family_handle_t *
pmdb_get_rocksdb_column_family_handle(void)
{
    /* NOTE:  do not cache handles until an revalidation method is in place to
     *   deal with stale handles from bulk recovery.
     */
    return PmdbCfHandleLookup(PMDB_COLUMN_FAMILY_NAME);
}

void
pmdb_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct pmdb_msg) <=
                        PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP);

    // enum PmdbOpType must fit into 'uint8_t pmdbrm_op'
    COMPILE_TIME_ASSERT(pmdb_op_any < (1 << (sizeof(uint8_t) * NBBY)));
}

#define PMDB_ARG_CHECK(op, rncr)                     \
    NIOVA_ASSERT(                                    \
        (rncr) &&                                    \
        (rncr)->rncr_type == op &&                   \
        ((rncr)->rncr_request ||                     \
         op == RAFT_NET_CLIENT_REQ_TYPE_COMMIT) &&   \
        (rncr)->rncr_reply &&                        \
        (rncr)->rncr_request_or_commit_data &&       \
        ((rncr)->rncr_request_or_commit_data_size >= \
         sizeof(struct pmdb_msg)) &&                 \
        (((char *)(rncr)->rncr_request->rcrm_data == \
          (rncr)->rncr_request_or_commit_data) ||    \
         op == RAFT_NET_CLIENT_REQ_TYPE_COMMIT) &&   \
        (rncr)->rncr_reply_data_max_size >= sizeof(struct pmdb_msg))

#define PMDB_CFH_MUST_GET()                      \
({                                               \
    rocksdb_column_family_handle_t *cfh =        \
        pmdb_get_rocksdb_column_family_handle(); \
                                                 \
    NIOVA_ASSERT(cfh);                           \
    cfh;                                         \
})

// For now, PMDB is using key-version 0.
#define PMDB_ENTRY_KEY_LEN sizeof(struct raft_net_client_user_key_v0)
#define PMDB_RNCUI_2_KEY(rncui) (const char *)&(rncui)->rncui_key.v0

static int
pmdb_object_lookup(const struct raft_net_client_user_id *rncui,
                   struct pmdb_object *obj, const int64_t current_raft_term)
{
    NIOVA_ASSERT(obj && rncui);

    size_t val_len = 0;
    char *err = NULL;
    int rc = -ENOENT;

    rocksdb_readoptions_t *read_opts = rocksdb_readoptions_create();
    if (!read_opts)
        return -ENOMEM;

    char *get_value = rocksdb_get_cf(pmdb_get_rocksdb_instance(), read_opts,
                                     PMDB_CFH_MUST_GET(),
                                     PMDB_RNCUI_2_KEY(rncui),
                                     PMDB_ENTRY_KEY_LEN, &val_len, &err);

    // Release rocksdb read opts
    rocksdb_readoptions_destroy(read_opts);

    PMDB_STR_DEBUG(LL_NOTIFY, rncui, "err=%s val=%p", err, get_value);

    if (err || !get_value)
        return rc; //XXX need a proper error code intepreter

    if (val_len != sizeof(struct pmdb_object))
    {
        PMDB_STR_DEBUG(LL_WARN, rncui, "invalid len (%zu), expected %zu",
                       val_len, sizeof(struct pmdb_object));

        rc = -EUCLEAN;
    }
    else
    {
        memcpy((void *)obj, get_value, sizeof(struct pmdb_object));
        rc = 0;
    }

    // Release buffer allocated by rocksdb C interface
    free(get_value);

    if (obj->pmdb_obj_pending_term > current_raft_term)
        rc = -EOVERFLOW;

    PMDB_OBJ_DEBUG((rc ? LL_WARN : LL_DEBUG), obj, "current_raft_term=%ld %s",
                   current_raft_term, strerror(-rc));

    return rc;
}

static void
pmdb_obj_to_reply(const struct pmdb_object *obj, struct pmdb_msg *reply,
                  const int64_t current_raft_term, const int32_t err)
{
    NIOVA_ASSERT(obj && reply);

    reply->pmdbrm_err = err;
    //XXx  reply->pmdbrm_user_id should have already been set
//    reply->pmdbrm_user_id = obj->pmdb_obj_rncui;
    reply->pmdbrm_op = pmdb_op_reply;
    reply->pmdbrm_write_seqno = obj->pmdb_obj_commit_seqno;

    // if either term value is -1 then write_pending is false;
    reply->pmdbrm_write_pending =
        (obj->pmdb_obj_pending_term == current_raft_term &&
         current_raft_term != ID_ANY_64bit) ? 1 : 0;
}

/**
 * pmdb_sm_handler_client_lookup - perform a key lookup in the PMDB column-
 *    family.
 * RETURN: 0 is always returned so that a reply will be delivered to the
 *    client.
 */
static int
pmdb_sm_handler_client_lookup(struct raft_net_client_request_handle *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    struct pmdb_object pmdb_obj = {0};

    struct pmdb_msg *pmdb_reply =
        RAFT_NET_MAP_RPC(pmdb_msg, rncr->rncr_reply);

    int rc = pmdb_object_lookup(&pmdb_req->pmdbrm_user_id, &pmdb_obj,
                                rncr->rncr_current_term);

    //XXX the 'rc' here may be for a system error from rocksDB
    pmdb_obj_to_reply(&pmdb_obj, pmdb_reply, rncr->rncr_current_term, rc);

    return 0;
}

static uint32_t
pmdb_get_current_version(void)
{
    return 0;
}

static void
pmdb_prep_raft_entry_write_obj(struct pmdb_object *obj, int64_t current_term)
{
    NIOVA_ASSERT(obj->pmdb_obj_version == pmdb_get_current_version());

    /* current-term of -1 means pmdb_prep_raft_entry_write_obj() is called in
     * apply context.  Otherwise, when called in write context, the object's
     * pending-term must be less than the current-term.
     */
    if (current_term != ID_ANY_64bit)
        NIOVA_ASSERT(obj->pmdb_obj_pending_term < current_term);

    obj->pmdb_obj_pending_term = current_term;

    pmdb_obj_crc_calc(obj);
}

static void
pmdb_prep_obj_write(struct raft_net_sm_write_supplements *ws,
                    const struct raft_net_client_user_id *rncui,
                    struct pmdb_object *obj, const int64_t term)
{
    NIOVA_ASSERT(ws && obj);

    pmdb_prep_raft_entry_write_obj(obj, term);

    PMDB_OBJ_DEBUG(LL_DEBUG, obj, "");

    raft_net_sm_write_supplement_add(
        ws, (void *)pmdb_get_rocksdb_column_family_handle(),
        NULL /* no callback needed yet */, PMDB_RNCUI_2_KEY(rncui),
        PMDB_ENTRY_KEY_LEN, (const char *)obj, sizeof(*obj));
}

static void
pmdb_prep_raft_entry_write(struct raft_net_client_request_handle *rncr,
                           struct pmdb_object *obj)
{
    NIOVA_ASSERT(rncr && obj);

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    pmdb_object_net_init(obj, rncr->rncr_client_uuid, rncr->rncr_msg_id);

    raft_net_client_request_handle_set_write_raft_entry(rncr);

    // Mark that the object is pending a write in this leader's term.
    pmdb_prep_obj_write(&rncr->rncr_sm_write_supp, &pmdb_req->pmdbrm_user_id,
                        obj, rncr->rncr_current_term);

    PMDB_OBJ_DEBUG(LL_DEBUG, obj, "");
}

static void
pmdb_prep_sm_apply_write(struct raft_net_client_request_handle *rncr,
                         struct pmdb_object *obj)
{
    NIOVA_ASSERT(rncr && obj);

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    // Increment the commit sequence by 1.
    obj->pmdb_obj_commit_seqno++;

    // Reset the pending term value with -1
    pmdb_prep_obj_write(&rncr->rncr_sm_write_supp, &pmdb_req->pmdbrm_user_id,
                        obj, ID_ANY_64bit);

    PMDB_OBJ_DEBUG(LL_DEBUG, obj, "");
}

static struct pmdb_cowr_sub_app *
pmdb_cowr_sub_app_construct(const struct pmdb_cowr_sub_app *in, void *arg)
{
    (void)arg;

    if (!in)
        return NULL;

    struct pmdb_cowr_sub_app *sa =
        niova_calloc_can_fail((size_t)1, sizeof(struct pmdb_cowr_sub_app));

    if (!sa)
        return NULL;

    raft_net_client_user_id_copy(&sa->pcwsa_rncui, &in->pcwsa_rncui);

    uuid_copy(sa->pcwsa_client_uuid, in->pcwsa_client_uuid);

    sa->pcwsa_current_term = in->pcwsa_current_term;

    return sa;
}

static int
pmdb_cowr_sub_app_destruct(struct pmdb_cowr_sub_app *destroy, void *arg)
{
    (void)arg;

    if (!destroy)
        return -EINVAL;

    niova_free(destroy);

    return 0;
}

static void
pmdb_cowr_sub_app_put(struct pmdb_cowr_sub_app *sa,
                      const char *caller_func, const int caller_lineno)
{
    SIMPLE_LOG_MSG(LL_DEBUG, "%s:%d", caller_func, caller_lineno);
    RT_PUT(pmdb_cowr_sub_app_tree, &pmdb_cowr_sub_apps, sa);
}

static void
pmdb_cowr_sub_app_release_all(void)
{
    struct pmdb_cowr_sub_app *sa =
        REF_TREE_MIN(pmdb_cowr_sub_app_tree, &pmdb_cowr_sub_apps,
                     pmdb_cowr_sub_app, pcwsa_rtentry);

    while (sa)
    {
        pmdb_cowr_sub_app_put(sa, __func__, __LINE__);
        pmdb_cowr_sub_app_put(sa, __func__, __LINE__);

        sa = REF_TREE_MIN(pmdb_cowr_sub_app_tree, &pmdb_cowr_sub_apps,
                          pmdb_cowr_sub_app, pcwsa_rtentry);
    }
    pmdb_cowr_tree_term = -1;
}


static struct pmdb_cowr_sub_app *
pmdb_cowr_sub_app_lookup(const struct raft_net_client_user_id *rncui,
                         const char *caller_func, const int caller_lineno)
{
    NIOVA_ASSERT(rncui);

    struct pmdb_cowr_sub_app *sa =
        RT_LOOKUP(pmdb_cowr_sub_app_tree, &pmdb_cowr_sub_apps,
                  (const struct pmdb_cowr_sub_app *)rncui);

    if (sa)
        SIMPLE_LOG_MSG(LL_DEBUG, "%s:%d", caller_func, caller_lineno);

    return sa;
}

static struct pmdb_cowr_sub_app *
pmdb_cowr_sub_app_add(const struct raft_net_client_user_id *rncui,
                      const uuid_t client_uuid, const int current_term,
                      int *ret_error,
                      const char *caller_func, const int caller_lineno)
{
    NIOVA_ASSERT(rncui);

    struct pmdb_cowr_sub_app cowr = {0};
    raft_net_client_user_id_copy(&cowr.pcwsa_rncui, rncui);
    uuid_copy(cowr.pcwsa_client_uuid, client_uuid);
    cowr.pcwsa_current_term = current_term;
    int error = 0;

    // If there are any stale entries in cowr RB tree, release them all.
    if (pmdb_cowr_tree_term != current_term)
        pmdb_cowr_sub_app_release_all();

    struct pmdb_cowr_sub_app *subapp = RT_GET_ADD(pmdb_cowr_sub_app_tree,
                                                  &pmdb_cowr_sub_apps, &cowr,
                                                  &error);

    if (!subapp)
    {
        LOG_MSG(LL_WARN, "Can not add RB entry pmdb_cowr_sub_app_add(): %s",
                strerror(-error));

        return NULL;
    }

    if (error) // The entry already existed
    {
        /*
         * -EALREADY indicates write request is already in coalesced buffer.
         * Convert the error to -EINPROGRESS as -EALREADY means write is
         * already committed in pmdb_sm_handler_client_write().
         */
        if (error == -EALREADY || error == -EEXIST)
        {
            PMDB_STR_DEBUG(LL_DEBUG, &subapp->pcwsa_rncui, "RNCUI already added");
            *ret_error = -EINPROGRESS;
            // If the different client is trying to use existing rncui.
            if (uuid_compare(subapp->pcwsa_client_uuid, client_uuid))
            {
                LOG_MSG(LL_DEBUG, "Different client trying out existing rncui");
                *ret_error = -EPERM;
            }
        }
        pmdb_cowr_sub_app_put(subapp, __func__, __LINE__);
        return NULL;
    }

    PMDB_STR_DEBUG(LL_DEBUG, &subapp->pcwsa_rncui, "RNCUI added successfully");
    NIOVA_ASSERT(pmdb_cowr_tree_term == -1 || pmdb_cowr_tree_term == current_term);

    if (pmdb_cowr_tree_term == -1)
        pmdb_cowr_tree_term = current_term;

    return subapp;
}

static struct pmdb_range_read_req *
pmdb_range_read_req_construct(const struct pmdb_range_read_req *in, void *arg)
{
    (void)arg;

    if (!in)
        return NULL;

    struct pmdb_range_read_req *rr =
        niova_calloc_can_fail((size_t)1, sizeof(struct pmdb_range_read_req));

    if (!rr)
        return NULL;

    rr->prrq_seq = in->prrq_seq;

    rr->prrq_term = in->prrq_term;
    rr->prrq_roptions = rocksdb_readoptions_create();
    rr->prrq_snap = rocksdb_create_snapshot(PmdbGetRocksDB());
    rocksdb_readoptions_set_snapshot(rr->prrq_roptions, rr->prrq_snap);

    // Get the timestamp for snapshot creation.
    niova_realtime_coarse_clock(&rr->prrq_snap_atime);
    CIRCLEQ_INSERT_TAIL(&prrq_queue, rr, prrq_lentry);

    return rr;
}

static int
pmdb_range_read_req_destruct(struct pmdb_range_read_req *destroy, void *arg)
{
    (void)arg;

    if (!destroy)
        return -EINVAL;

    //release snapshot
    rocksdb_readoptions_set_snapshot(destroy->prrq_roptions, NULL);
    rocksdb_readoptions_destroy(destroy->prrq_roptions);
    rocksdb_release_snapshot(PmdbGetRocksDB(), destroy->prrq_snap);

    // Remove the entry from request list as well
    CIRCLEQ_REMOVE(&prrq_queue, destroy, prrq_lentry);

    niova_free(destroy);

    return 0;
}

static void
pmdb_range_read_req_put(struct pmdb_range_read_req *rr,
                        const char *caller_func, const int caller_lineno)
{
    SIMPLE_LOG_MSG(LL_DEBUG, "%s:%d", caller_func, caller_lineno);
    RT_PUT(pmdb_range_read_req_tree, &pmdb_range_read_req, rr);
}

static void
pmdb_range_read_release_old_snapshots(void)
{
    struct pmdb_range_read_req *rr, *tmp_rr;
    struct timespec now;
    niova_realtime_coarse_clock(&now);

    if (FAULT_INJECT(pmdb_range_read_keep_old_snapshot))
    {
        SIMPLE_LOG_MSG(LL_DEBUG,
                       "Dont destroy the older snapshot as fault ineject is set");
        return;
    }

    CIRCLEQ_FOREACH_SAFE(rr, &prrq_queue, prrq_lentry, tmp_rr)
    {
        /* If snapshot was open for more than 60secs, release the snapshot */
        if ((now.tv_sec - rr->prrq_snap_atime.tv_sec) >=
             PMDB_SNAPSHOT_MAX_OPEN_TIME_SEC)
        {
             pmdb_range_read_req_put(rr, __func__, __LINE__);
        }
        else
            // List sorted with time.
            break;
    }
}

static void
pmdb_range_read_req_release_all(void)
{
    struct pmdb_range_read_req *rr =
        REF_TREE_MIN(pmdb_range_read_req_tree, &pmdb_range_read_req,
                     pmdb_range_read_req, prrq_rtentry);

    while (rr)
    {
        pmdb_range_read_req_put(rr, __func__, __LINE__);

        rr = REF_TREE_MIN(pmdb_range_read_req_tree, &pmdb_range_read_req,
                          pmdb_range_read_req, prrq_rtentry);
    }
    pmdb_range_read_tree_term = -1;
}

static struct pmdb_range_read_req *
pmdb_range_read_req_lookup(const uint64_t seq_number,
                           const char *caller_func, const int caller_lineno)
{
    struct pmdb_range_read_req *rr =
        RT_LOOKUP(pmdb_range_read_req_tree, &pmdb_range_read_req,
                  (const struct pmdb_range_read_req *)&seq_number);

    if (rr)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "%s:%d", caller_func, caller_lineno);
        /*
         * As snapshot is being reused, move it to prrq_queue tail and change
         * its access time.
         */
        niova_realtime_coarse_clock(&rr->prrq_snap_atime);
        CIRCLEQ_REMOVE(&prrq_queue, rr, prrq_lentry);
        CIRCLEQ_INSERT_TAIL(&prrq_queue, rr, prrq_lentry);
    }

    return rr;
}

static struct pmdb_range_read_req *
pmdb_range_read_req_add(const uint64_t seq_number,
                        const int64_t current_term,
                        const char *caller_func, const int caller_lineno)
{
    // If there are any stale entries in range read req RB tree, release them all.
    if (pmdb_range_read_tree_term != current_term)
        pmdb_range_read_req_release_all();

    if (pmdb_range_read_tree_term == -1)
        pmdb_range_read_tree_term = current_term;

    NIOVA_ASSERT(pmdb_range_read_tree_term == current_term);

    struct pmdb_range_read_req prrq = {0};
    prrq.prrq_seq = seq_number;
    prrq.prrq_term = pmdb_range_read_tree_term;

    int error = 0;

    prrq.prrq_snap = NULL;

    struct pmdb_range_read_req *rr_req = RT_GET_ADD(pmdb_range_read_req_tree,
                                                    &pmdb_range_read_req, &prrq,
                                                    &error);

    if (!rr_req)
    {
        LOG_MSG(LL_WARN, "Can not add RB entry pmdb_range_read_req_add(): %s",
                strerror(-error));

        return NULL;
    }

    if (error) // The entry already existed
    {
        pmdb_range_read_req_put(rr_req, __func__, __LINE__);
        SIMPLE_LOG_MSG(LL_WARN,
                       "Snapshot for sequence number (%lu) already exists: %p",
                       rr_req->prrq_seq,
                       rr_req->prrq_snap);
        return rr_req;
    }

    SIMPLE_LOG_MSG(LL_WARN, "Snapshot for sequence number (%lu) added successfully",
                   rr_req->prrq_seq);

    return rr_req;
}

static void
pumicedb_init_cb_args(const struct raft_net_client_user_id *app_id,
                      const void *req_buf, size_t req_bufsz,
                      char *reply_buf, size_t reply_bufsz,
                      uint32_t bootup_peer,
                      int *continue_wr, void *pmdb_handler,
                      void *user_data,
                      struct pumicedb_cb_cargs *args)
{
    args->pcb_userid = app_id;
    args->pcb_req_buf = req_buf;
    args->pcb_req_bufsz = req_bufsz;
    args->pcb_reply_buf = reply_buf;
    args->pcb_reply_bufsz = reply_bufsz;
    args->pcb_bootup_peer = bootup_peer;
    args->pcb_continue_wr = continue_wr;
    args->pcb_pmdb_handler = pmdb_handler;
    args->pcb_user_data = user_data;
}

static int
pmdb_write_prep_cb(struct raft_net_client_request_handle *rncr,
                   int *continue_wr)
{
    if (!pmdbApi->pmdb_write_prep)
    {
        return 0;
    }

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    const struct raft_net_client_user_id *rncui = &pmdb_req->pmdbrm_user_id;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    struct pmdb_msg *pmdb_reply = (struct pmdb_msg *)reply->rcrm_data;
    const size_t max_reply_size =
        rncr->rncr_reply_data_max_size -
        PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP;
    struct pumicedb_cb_cargs write_prep_cb_args;
    ssize_t rc = 0;

    pumicedb_init_cb_args(rncui, pmdb_req->pmdbrm_data,
                          pmdb_req->pmdbrm_data_size,
                          pmdb_reply ?
                          pmdb_reply->pmdbrm_data : NULL,
                          max_reply_size, 0,
                          continue_wr, NULL,
                          pmdb_user_data,
                          &write_prep_cb_args);

    rc = pmdbApi->pmdb_write_prep(&write_prep_cb_args);
    if (rc < 0)
    {
        raft_client_net_request_handle_error_set(rncr,
                                                 -EPERM,
                                                 0, -EPERM);
        rc = -EPERM;
    }
    else if (rc >= 0 && !*continue_wr)
    {
         // Write prepare was successful but application don't want
         // to continue with raft write.
         raft_client_net_request_handle_error_set(rncr,
                                                  -EALREADY,
                                                  0, 0);
         pmdb_reply->pmdbrm_data_size = rc;
         reply->rcrm_data_size += (uint32_t)rc;
    }

    return rc >= 0 ? 0 : -1;
}

/**
 * pmdb_sm_handler_client_write - lookup the object and ensure that the
 *    requested write sequence number is consistent with the pmdb-object.
 *
 * Note:  this function is only called by the raft leader, or a raft instance
 *    which believes its a viable leader.  In most cases, the ensuing write
 *    of the pmdb_object into the pumiceDB column family (where the pmdb_object
 *    has set pmdb_obj_pending_term to block out other writes) will only occur
 *    on this leader.  It will not occur on the followers!  The followers will
 *    eventually receive the pmdb_msg payload but they will not execute
 *    the intermediate step of marking the object to prevent new writes.  At
 *    commit time, each follower will eventually call
 *    pmdb_sm_handler_pmdb_sm_apply() which places the object into the
 *    pumiceDB column family with a clear pmdb_obj_pending_term value.
 * RETURN:  Returning without an error and with rncr_write_raft_entry=false
 *    will cause an immediate reply to the client.  Returning any non-zero
 *    value causes the request to terminate immediately without any reply being
 *    issued.
 */
static int
pmdb_sm_handler_client_write(struct raft_net_client_request_handle *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_WRITE, rncr);

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    const struct raft_net_client_user_id *rncui = &pmdb_req->pmdbrm_user_id;
    struct pmdb_object obj = {0};
    bool new_object = false;
    int64_t prev_pending_term = -1;

    int rc = pmdb_object_lookup(rncui, &obj,
                                rncr->rncr_current_term);
    if (rc)
    {
        if (rc == -ENOENT)
        {
            pmdb_object_init(&obj, pmdb_get_current_version(),
                             rncui);
            rc = 0;
            new_object = true;
        }
        else
        {
            PMDB_STR_DEBUG(LL_NOTIFY, rncui,
                           "pmdb_object_lookup(): %s", strerror(-rc));

            /* This appears to be a system error.  Mark it and reply to the
             * client.
             */
            raft_client_net_request_handle_error_set(rncr, rc, 0, rc);

            return 0;
        }
    }
    else
    {
        PMDB_OBJ_DEBUG(LL_NOTIFY, &obj, "obj exists");
    }

    /* Check if the request was already committed and applied.  A commit-seqno
     * of ID_ANY_64bit means the object has previously attempted a write but
     * that write did not yet (or ever) commit.
     */
    if (pmdb_req->pmdbrm_write_seqno <= obj.pmdb_obj_commit_seqno &&
        obj.pmdb_obj_commit_seqno != ID_ANY_64bit)
    {
        raft_client_net_request_handle_error_set(rncr, -EALREADY, 0, 0);
    }
    else if (pmdb_req->pmdbrm_write_seqno == (obj.pmdb_obj_commit_seqno + 1))
    {
        /* Check if request has already been placed into the log but not yet
         * applied.  Here, the client's request has been accepted but not
         * yet completed and the client has retried the request.
         */
        prev_pending_term = obj.pmdb_obj_pending_term;

        /* -EINPROGRESS is treated as 'system error' at this time and this
         * error does not reach the client's application layer.  The client
         * will retry the operation until it succeeds (due to the condition
         * above) or times out.
         */
        if (obj.pmdb_obj_pending_term == rncr->rncr_current_term)
        {
            raft_client_net_request_handle_error_set(rncr, -EINPROGRESS,
                                                     -EINPROGRESS, 0);
        }
        else
        {
            int error = 0;
            struct pmdb_cowr_sub_app *cowr_sa =
            pmdb_cowr_sub_app_add(rncui,
                                  rncr->rncr_client_uuid,
                                  rncr->rncr_current_term,
                                  &error, __func__,
                                  __LINE__);
            if (!cowr_sa)
                raft_client_net_request_handle_error_set(
                        rncr, error, 0, error);

            else // Request sequence test passes, will enter the raft log.
            {
                int continue_wr = 1;
                rc = pmdb_write_prep_cb(rncr, &continue_wr);
                // If write_prep return success and allow to continue raft write.
                if (!rc && continue_wr)
                    pmdb_prep_raft_entry_write(rncr, &obj);
            }
        }
    }
    else // Request sequence is too far ahead
    {
        rc = -EBADE;
        raft_client_net_request_handle_error_set(rncr, rc, 0, 0);
    }

    // Stash the obj metadata into the reply
    struct pmdb_msg *pmdb_reply = RAFT_NET_MAP_RPC(pmdb_msg, rncr->rncr_reply);
    pmdb_obj_to_reply(&obj, pmdb_reply, rncr->rncr_current_term, rc);

    PMDB_OBJ_DEBUG((rncr->rncr_op_error == -EBADE ? LL_NOTIFY : LL_DEBUG),
                   &obj, "op-err=%s new-object=%s (ppt=%ld)",
                   strerror(-rncr->rncr_op_error), new_object ? "yes" : "no",
                   prev_pending_term);

    return 0;
}

static int
pmdb_sm_handler_client_read(struct raft_net_client_request_handle *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)req->rcrm_data;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    struct pmdb_msg *pmdb_reply = (struct pmdb_msg *)reply->rcrm_data;

    NIOVA_ASSERT(pmdb_req->pmdbrm_data_size <= PMDB_MAX_APP_RPC_PAYLOAD_SIZE);

    const size_t max_reply_size =
        rncr->rncr_reply_data_max_size - PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP;

    // Lookup the 'root' object
    struct pmdb_object obj = {0};
    ssize_t rrc = pmdb_object_lookup(&pmdb_req->pmdbrm_user_id, &obj,
                                    rncr->rncr_current_term);


    if (pmdb_rncui_is_read_any(&pmdb_req->pmdbrm_user_id))
    {
        LOG_MSG(LL_DEBUG, "rncui is read any");
    }

    if (!rrc || pmdb_rncui_is_read_any(&pmdb_req->pmdbrm_user_id))   // Ok.  Continue to read operation
    {
        struct pumicedb_cb_cargs read_cb_args;
        pumicedb_init_cb_args(&pmdb_req->pmdbrm_user_id,
                              pmdb_req->pmdbrm_data,
                              pmdb_req->pmdbrm_data_size,
                              pmdb_reply->pmdbrm_data, max_reply_size, 0,
                              NULL, NULL,
                              pmdb_user_data,
                              &read_cb_args);

        // FIXME Get the current term value here.
        if (rncr->rncr_current_term != pmdb_current_term)
            pmdb_current_term = rncr->rncr_current_term;

        // If there are stale range read snapshot, destroy those.
        rrc = pmdbApi->pmdb_read(&read_cb_args);
    }
    //XXX fault injection needed
    if (rrc < 0)
    {
        pmdb_reply->pmdbrm_data_size = 0;
        raft_client_net_request_handle_error_set(rncr, rrc, 0, rrc);

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req,
                            "pmdbApi::read(): %s", strerror(rrc));
    }
    else if (rrc > (ssize_t)max_reply_size)
    {
        raft_client_net_request_handle_error_set(rncr, -E2BIG, 0, -E2BIG);
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req,
                            "pmdbApi::read(): reply too large (%zd)", rrc);
    }
    else
    {
        // Add the reply size to the RPC reply
        reply->rcrm_data_size += (uint32_t)rrc;

        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_DEBUG, req,
                            "pmdbApi::read(): reply-size=%zd", rrc);
    }

    pmdb_obj_to_reply(&obj, pmdb_reply, rncr->rncr_current_term,
                      rncr->rncr_op_error);

    return 0;
}

static void
pmdb_reply_init(const struct pmdb_msg *req, struct pmdb_msg *reply)
{
    NIOVA_ASSERT(req && reply);

    reply->pmdbrm_data_size = 0;
    reply->pmdbrm_op = req->pmdbrm_op;

    raft_net_client_user_id_copy(&reply->pmdbrm_user_id, &req->pmdbrm_user_id);
}

/**
 * pmdb_sm_handler_client_rw_op - interprets and executes the command provided
 *    in the raft net client request.  Note that this function must set
 *    rncr_type so that the caller, which does not have the ability to
 *    interpret the request, can be informed of the request type.
 */
static int
pmdb_sm_handler_client_rw_op(struct raft_net_client_request_handle *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE &&
                 rncr->rncr_request && rncr->rncr_reply &&
                 rncr->rncr_request_or_commit_data_size >=
                 sizeof(struct pmdb_msg));

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    const enum PmdbOpType op = pmdb_req->pmdbrm_op;

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, rncr->rncr_request, "op=%u", op);

    switch (op)
    {
    case pmdb_op_noop:
        return 0; // noop should be harmless

    case pmdb_op_read:
        // Check if there are any stale or really old snapshot in the range read
        // tree and release them.
        pmdb_range_read_release_old_snapshots();

        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_READ;
        return pmdb_sm_handler_client_read(rncr);

    case pmdb_op_write:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_WRITE;
        return pmdb_sm_handler_client_write(rncr);

    case pmdb_op_lookup: // type of "read" which does not enter the app API
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_READ;
        return pmdb_sm_handler_client_lookup(rncr);

    default:
        break;
    }

    return -EOPNOTSUPP;
}

/**
 * pmdb_init_net_client_request_from_obj - prepares the rncr for a possible
 *   reply to a client.
 */
static raft_server_sm_apply_cb_t
pmdb_init_net_client_request_from_obj(
    struct raft_net_client_request_handle *rncr,
    const struct pmdb_object *pmdb_obj)
{
    NIOVA_ASSERT(rncr && pmdb_obj);

    raft_net_client_request_handle_set_reply_info(
        rncr, pmdb_obj->pmdb_obj_client_uuid, pmdb_obj->pmdb_obj_msg_id);
}

static raft_server_sm_apply_cb_t
pmdb_sm_handler_pmdb_sm_apply_remove_range_read_tree_item(
    struct raft_net_client_request_handle *rncr)
{
    NIOVA_ASSERT(rncr);

    if (!rncr->rncr_is_leader)
    {
        /*
         * remove any stale range read snapshot entries from the tree
         */
        pmdb_range_read_req_release_all();
    }
    return;
}

static raft_server_sm_apply_cb_t
pmdb_sm_handler_pmdb_sm_apply_remove_coalesce_tree_item(
    const struct pmdb_msg *pmdb_req,
    struct raft_net_client_request_handle *rncr)
{
    NIOVA_ASSERT(pmdb_req && rncr);

    if (!rncr->rncr_is_leader)
    {
        /* Ensure the tree is empty.  If this peer was previously a leader
         * remove any / all stale entries.
         */
        pmdb_cowr_sub_app_release_all();
        return;
    }

    NIOVA_ASSERT(pmdb_cowr_tree_term == -1 || pmdb_cowr_tree_term == rncr->rncr_current_term);

    // Else 'leader'
    /* We use an RB_TREE lookup here since the pointer cannot easily be
     * stored elsewhere.  (ie the rncr presented here is not the one used
     * in 'write').
     */
    struct pmdb_cowr_sub_app *cowr_sa =
        pmdb_cowr_sub_app_lookup(&pmdb_req->pmdbrm_user_id, __func__,
                                 __LINE__);

    if (cowr_sa) // release ref on the rncui from coalesced write RB tree
    {
        const struct raft_net_client_user_id *rncui =
            &pmdb_req->pmdbrm_user_id;

        // Guarantee that the cowr_sa affiliation
        if (uuid_compare(cowr_sa->pcwsa_client_uuid,
                         rncr->rncr_client_uuid) ||
            raft_net_client_user_id_cmp(&cowr_sa->pcwsa_rncui, rncui) ||
            cowr_sa->pcwsa_current_term != rncr->rncr_current_term)
        {
            // Print some details to the log before aborting
            DECLARE_AND_INIT_UUID_STR(cowr_uuid,
                                      cowr_sa->pcwsa_client_uuid);
            DECLARE_AND_INIT_UUID_STR(client_uuid, rncr->rncr_client_uuid);

            char cowr_rncui_str[129];
            char rncr_rncui_str[129];

            raft_net_client_user_id_to_string(&cowr_sa->pcwsa_rncui,
                                              cowr_rncui_str, 129);
            raft_net_client_user_id_to_string(rncui, rncr_rncui_str, 129);

            FATAL_IF(
                1, "unmatching cowr client / rncr client (%s, %s),"
                " or cowr rncui / rncr rncui (%s, %s),"
                " or cowr_sa term: %ld/ rncr term: %ld",
                cowr_uuid, client_uuid, cowr_rncui_str, rncr_rncui_str,
                cowr_sa->pcwsa_current_term, rncr->rncr_current_term);
        }

        pmdb_cowr_sub_app_put(cowr_sa, __func__, __LINE__);
        pmdb_cowr_sub_app_put(cowr_sa, __func__, __LINE__);
    }
}

/**
 * pmdb_sm_handler_pmdb_sm_apply - ri_server_sm_request apply cb for pumiceDB.
 *   This function has 2 primary roles:  1) updating (if leader), or creating
 *   (if follower), the pmdb_object for this request.  2) calling into the
 *   pmdb app layer to obtain any KVs that it would like to have written.
 *   The underlying raft layer, via raft_server_state_machine_apply(), is
 *   reponsible for writing these KVs into rocksDB through the function,
 *   raft_server_sm_apply_opt().
 */
static raft_server_sm_apply_cb_int_t
pmdb_sm_handler_pmdb_sm_apply(const struct pmdb_msg *pmdb_req,
                              struct raft_net_client_request_handle *rncr)
{
    if (!pmdb_req || !rncr)
        return -EINVAL;

    const struct raft_net_client_user_id *rncui = &pmdb_req->pmdbrm_user_id;

    struct pmdb_object obj = {0};

    int rc = pmdb_object_lookup(rncui, &obj, rncr->rncr_current_term);
    if (rc)
    {
        PMDB_STR_DEBUG(((rc == -ENOENT) ? LL_DEBUG : LL_NOTIFY), rncui,
                       "pmdb_object_lookup(): %s", strerror(-rc));

        /* Since the KV is being rewritten, replace the errors with -ESTALE
         * only for leader (as pmdb object was written only on leader as marker in
         * the write phase. For followers it would fail with ENOENT)
         * so that upper layer will not attempt to issue a reply.
         */
        if (rncr->rncr_is_leader)
            rc = -ESTALE;

        /* Initialize the object as best we can given that reply information
         * is not present since this raft instance did not accept the initial
         * write.
         */
        pmdb_object_init(&obj, pmdb_get_current_version(),
                         &pmdb_req->pmdbrm_user_id);
    }

    pmdb_init_net_client_request_from_obj(rncr, &obj);

    /* The object receiving the apply must have its pending_term value reset.
     * pmdb_prep_sm_apply_write() will cause a KV to be placed into the write
     * supplement.
     */
    pmdb_prep_sm_apply_write(rncr, &obj);

    struct raft_net_sm_write_supplements *ws = &rncr->rncr_sm_write_supp;
    struct pmdb_apply_handle pah = {.pah_rncui = rncui, .pah_ws = ws};
    struct pmdb_msg *pmdb_reply = NULL;
    const size_t max_reply_size =
        rncr->rncr_reply_data_max_size - PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP;
    struct pumicedb_cb_cargs apply_args;
    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    if (rncr->rncr_is_leader)
        pmdb_reply = (struct pmdb_msg *)reply->rcrm_data;

    pumicedb_init_cb_args(rncui, pmdb_req->pmdbrm_data,
                          pmdb_req->pmdbrm_data_size, pmdb_reply ?
                          pmdb_reply->pmdbrm_data : NULL,
                          max_reply_size, 0, NULL, (void *)&pah,
                          pmdb_user_data, &apply_args);


    // Call into the application so it may emplace its own KVs.
    ssize_t apply_rc =
        pmdbApi->pmdb_apply(&apply_args);

    // rc of 0 means the client will get a reply and removal of coalesced
    // tree item only leader should send the reply back to client.
    if (!rc)
    {
        if (rncr->rncr_is_leader)
        {
            // Add the reply size to the RPC reply
            reply->rcrm_data_size += (uint32_t)apply_rc;

            pmdb_reply->pmdbrm_data_size = apply_rc >= 0 ?
                                           (uint32_t)apply_rc : 0;
            // Pass in ID_ANY_64bit since this is a reply.
            pmdb_obj_to_reply(&obj, pmdb_reply, ID_ANY_64bit,
                              apply_rc < 0 ? apply_rc: 0);
        }
        pmdb_sm_handler_pmdb_sm_apply_remove_coalesce_tree_item(pmdb_req, rncr);
        pmdb_sm_handler_pmdb_sm_apply_remove_range_read_tree_item(rncr);
    }

    return rc;
}

static int
pmdb_sm_handler_pmdb_req_check(const struct pmdb_msg *pmdb_req)
{
    if (pmdb_req->pmdbrm_data_size > PMDB_MAX_APP_RPC_PAYLOAD_SIZE)
        return -EINVAL;

    return 0;
}

static int
pmdb_sm_handler(struct raft_net_client_request_handle *rncr)
{
    if (!rncr || !rncr->rncr_request_or_commit_data ||
        raft_net_client_request_handle_writes_raft_entry(rncr))
        return -EINVAL;

    else if (rncr->rncr_request_or_commit_data_size < sizeof(struct pmdb_msg))
        return -EBADMSG;

    const struct pmdb_msg *pmdb_req =
        (const struct pmdb_msg *)rncr->rncr_request_or_commit_data;

    if (rncr->rncr_request) // otherwise, this is an apply operation
        DBG_RAFT_CLIENT_RPC(LL_DEBUG, rncr->rncr_request, "");

    if (pmdb_net_calc_rpc_msg_size(pmdb_req) !=
        rncr->rncr_request_or_commit_data_size)
        return -EMSGSIZE;

    /* Mapping of the reply buffer should not fail but it's ok if a reply
     * is not issued.
     */
    struct pmdb_msg *pmdb_reply =
        (struct pmdb_msg *)
        raft_net_client_request_handle_reply_data_map(
            rncr, sizeof(struct pmdb_msg));

    if (pmdb_reply)
        pmdb_reply_init(pmdb_req, pmdb_reply);

    int rc = 0;

    switch (rncr->rncr_type)
    {
    case RAFT_NET_CLIENT_REQ_TYPE_READ:  // fall through
    case RAFT_NET_CLIENT_REQ_TYPE_WRITE:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_NONE; // fall through

    case RAFT_NET_CLIENT_REQ_TYPE_NONE:
    {
        if (rncr->rncr_reply_data_max_size < sizeof(struct pmdb_msg))
            return -ENOSPC;

        rc = pmdb_sm_handler_pmdb_req_check(pmdb_req);
        if (rc)
        {
            raft_client_net_request_handle_error_set(rncr, rc, 0, rc);

            // There's a problem with the application RPC request
            DBG_RAFT_CLIENT_RPC(LL_NOTIFY, rncr->rncr_request,
                                "pmdb_sm_handler_pmdb_req_check(): %s",
                                strerror(-rc));
            return 0;
        }

        return pmdb_sm_handler_client_rw_op(rncr);
    }

    case RAFT_NET_CLIENT_REQ_TYPE_COMMIT:
        return pmdb_sm_handler_pmdb_sm_apply(pmdb_req, rncr);

    default:
        break;
    }

    return -EOPNOTSUPP;
}

static int
pmdb_handle_verify(const struct raft_net_client_user_id *app_id,
                   const struct pmdb_apply_handle *pah)
{
    return (!app_id || !pah || !pah->pah_rncui || !pah->pah_ws ||
            app_id != pah->pah_rncui ||
            raft_net_client_user_id_cmp(app_id, pah->pah_rncui)) ? -EINVAL : 0;
}

static void
pmdb_ref_tree_release_all(void)
{
    pmdb_cowr_sub_app_release_all();
    pmdb_range_read_req_release_all();
}

static void
pmdb_init_peer_handler(uint32_t bootup_peer)
{
    if (!bootup_peer)
        // Release entries from coalesced wr tree and range read tree
        pmdb_ref_tree_release_all();

    // Give control to application to perform any cleanup/initialization
    // on leader.
    struct pumicedb_cb_cargs init_leader_cb_args;

    if (pmdbApi->pmdb_init_peer)
    {
        pumicedb_init_cb_args(NULL, NULL, 0, NULL, 0, bootup_peer, NULL, NULL,
                              pmdb_user_data,
                              &init_leader_cb_args);
        pmdbApi->pmdb_init_peer(&init_leader_cb_args);
    }
}

static void
pmdb_cleanup_peer_handler(void)
{
    struct pumicedb_cb_cargs init_leader_cb_args;

    if (pmdbApi->pmdb_cleanup_peer)
    {
        pumicedb_init_cb_args(NULL, NULL, 0, NULL, 0, 0, NULL, NULL,
                              pmdb_user_data,
                              &init_leader_cb_args);
        pmdbApi->pmdb_cleanup_peer(&init_leader_cb_args);
    }
}

void
PmdbPutRoptionsWithSnapshot(const uint64_t seq_number)
{
    struct pmdb_range_read_req *prrq;

    prrq = pmdb_range_read_req_lookup(seq_number, __func__, __LINE__);
    if (!prrq)
    {
        SIMPLE_LOG_MSG(LL_WARN,
                       "There is no snapshot with seq: %lu to release",
                       seq_number);
        return;
    }

    // One put is for lookup above and another put is the actual reference drop.
    pmdb_range_read_req_put(prrq, __func__, __LINE__);
    if (FAULT_INJECT(pmdb_range_read_keep_snapshot) &&
        prrq->prrq_rtentry.rte_ref_cnt == 1)
        return;

    pmdb_range_read_req_put(prrq, __func__, __LINE__);
}

rocksdb_readoptions_t *
PmdbGetRoptionsWithSnapshot(const uint64_t seq_number,
                            uint64_t *ret_seq)
{
    struct pmdb_range_read_req *prrq = NULL;


    // Check if snapshot with the given seq_number is already created.
    if (seq_number >= 0)
        prrq = pmdb_range_read_req_lookup(seq_number, __func__, __LINE__);

    if (!prrq)
    {
        // Get the latest sequence number and create snapshot against it.
        uint64_t new_seq = rocksdb_get_latest_sequence_number(PmdbGetRocksDB());
        prrq = pmdb_range_read_req_add(new_seq, pmdb_current_term, __func__,
                                       __LINE__);
    }

    // Return the sequence numner if the new snapshot needs to be created as
    // original snapshot was destroyed.
    *ret_seq = prrq->prrq_seq;

    return prrq->prrq_roptions;
}

int PmdbGetLeaderTimeStamp(struct raft_leader_ts *ts)
{
    return raft_server_get_leader_ts(ts);
}

/**
 * PmdbWriteKV - to be called by the pumice-enabled application in 'apply'
 *    context only.  This call is used by the application to stage KVs for
 *    writing into rocksDB.  KVs added within a single instance of the 'apply'
 *    callback are atomically written to rocksDB.
 * @app_id:  identifier of the application instance
 * @pmdb_handle:  the handle which was provided from pumice_db to the apply
 *    callback.
 * @key:  name of the key
 * @key_len:  length of the key
 * @value:  value contents
 * @value_len:  length of value contents
 * @comp_cb:  optional callback which is issued following the rocksDB write
 *    operation.
 * @app_handle:  a handle pointer which belongs to the application.  This same
 *    pointer is returned via comp_cb().  Note, that at this time, PMDB assumes
 *    this handle is a pointer to a column family.
 */
int
PmdbWriteKV(const struct raft_net_client_user_id *app_id, void *pmdb_handle,
            const char *key, size_t key_len, const char *value,
            size_t value_len, void (*comp_cb)(void *), void *app_handle)
{
    struct pmdb_apply_handle *pah = (struct pmdb_apply_handle *)pmdb_handle;

    if (!key || !key_len || pmdb_handle_verify(app_id, pah))
        return -EINVAL;

    NIOVA_ASSERT(pah);

    return raft_net_sm_write_supplement_add(pah->pah_ws, app_handle, comp_cb,
                                            key, key_len, value, value_len);
}

/**
 * PmdbExec - blocking API call used by a pumice-enabled application which
 *    starts the underlying raft process and waits for incoming requests.
 * @raft_uuid_str:  UUID of raft
 * @raft_instance_uuid_str:  UUID of this specific raft peer
 * @pmdb_api:  Function callbacks for read and apply.
 */
static int
_PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api, const char *cf_names[],
         int num_cf_names, bool use_synchronous_writes,
         bool use_coalesced_writes)
{
    pmdbApi = pmdb_api;

    if (!raft_uuid_str || !raft_instance_uuid_str || !pmdb_api ||
        !pmdb_api->pmdb_apply || !pmdb_api->pmdb_read)
        return -EINVAL;

    REF_TREE_INIT(&pmdb_cowr_sub_apps, pmdb_cowr_sub_app_construct,
                  pmdb_cowr_sub_app_destruct, NULL);

    REF_TREE_INIT(&pmdb_range_read_req, pmdb_range_read_req_construct,
                  pmdb_range_read_req_destruct, NULL);

    CIRCLEQ_INIT(&prrq_queue);

    int rc = raft_server_rocksdb_add_cf_name(
        &pmdbCFT, PMDB_COLUMN_FAMILY_NAME, strlen(PMDB_COLUMN_FAMILY_NAME));

    FATAL_IF((rc), "raft_server_rocksdb_add_cf_name() %s", strerror(-rc));

    for (int i = 0; i < num_cf_names; i++)
    {
        rc = raft_server_rocksdb_add_cf_name(
            &pmdbCFT, cf_names[i],
            strnlen(cf_names[i], RAFT_ROCKSDB_MAX_CF_NAME_LEN));
        if (rc)
            return rc;
    }

    enum raft_instance_options opts =
        (RAFT_INSTANCE_OPTIONS_AUTO_CHECKPOINT |
         (use_synchronous_writes ? RAFT_INSTANCE_OPTIONS_SYNC_WRITES : 0) |
         (use_coalesced_writes ? RAFT_INSTANCE_OPTIONS_COALESCED_WRITES: 0));

    rc = raft_server_instance_run(raft_uuid_str, raft_instance_uuid_str,
                                  pmdb_sm_handler,
                                  pmdb_init_peer_handler,
                                  pmdb_cleanup_peer_handler,
                                  RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP,
                                  opts, &pmdbCFT);

    raft_server_rocksdb_release_cf_table(&pmdbCFT);

    return rc;
}

int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api, const char *cf_names[],
         int num_cf_names, bool use_synchronous_writes,
         bool use_coalesced_writes,
         void *user_data)
{
    pmdb_user_data = user_data;
    return _PmdbExec(raft_uuid_str, raft_instance_uuid_str, pmdb_api, cf_names,
                     num_cf_names, use_synchronous_writes,
                     use_coalesced_writes);
}

/**
 * PmdbClose - called from application context to shutdown the pumicedb exec
 *   thread.
 */
int
PmdbClose(void)
{
    return raft_net_instance_shutdown(raft_net_get_instance());
}

rocksdb_t *
PmdbGetRocksDB(void)
{
    return pmdb_get_rocksdb_instance();
}

const char *
PmdbRncui2Key(const struct raft_net_client_user_id *rncui)
{
	return (const char *)&(rncui)->rncui_key.v0;
}

size_t
PmdbEntryKeyLen(void)
{
	return sizeof(struct raft_net_client_user_key_v0);
}
