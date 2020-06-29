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
#include "registry.h"

LREG_ROOT_ENTRY_GENERATE(pumicedb_entry, LREG_USER_TYPE_RAFT);
REGISTRY_ENTRY_FILE_GENERATE;

#define PMDB_COLUMN_FAMILY_NAME "pumiceDB_private"

static const struct PmdbAPI *pmdbApi;
static rocksdb_column_family_handle_t *pmdbRocksdbCFH;
static rocksdb_readoptions_t *pmdbRocksdbReadOpts;

struct pmdb_obj_extras_v0
{
    uint64_t pmdb_oextra_commit_term;
    uuid_t   pmdb_oextra_commit_uuid; // UUID of leader at the time
};

/* XXXXXX there's an inconsistent usage model around this pmdb_object - the
issue is that the leader creates this object in his namespace in raft's initial
write phase, but the followers won't create their entries until the SM apply
phase.  This means that the
problem is that these objects are only built on the leader and are not
replicated to the followers.   This means that the set of pmdb_object held by
each raft peer will be incomplete - each peer only accumulates entries which
were written during its term as leader.  Therefore, the
XXX
 */

/**
 * pmdb_object - object which is stored in the value contents of a RocksDB KV.
 *    The object's role is to store pending write request state for the app
 *    request.  The contents serve to detect requests which are being retried
 *    or have already been committed, as well as containing the info necessary
 *    for formulating and directing a reply.
 * @pmdb_obj_crc:  checksum for the object
 * @pmdb_obj_version:  versioner for the object (currently not used)
 * @pmdb_obj_commit_seqno:  the pumicedb sequence number of this request.
 *   PumiceDB stipulate that each "application" (as determined by the contents
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
 * @pmdb_obj_remote_addr:  The IP information taken from the original request.
 *   This is used to direct the reply, assuming that the term which handled the
 *   write is current.
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
    struct sockaddr_in             pmdb_obj_remote_addr;
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


#define PMDB_OBJ_DEBUG(log_level, pmdb_rncui, pmdb_obj, fmt, ...)       \
    {                                                                   \
        char __uuid_str[UUID_STR_LEN];                                  \
        uuid_unparse(RAFT_NET_CLIENT_USER_ID_2_UUID(pmdb_rncui, 0, 0),  \
                     __uuid_str);                                       \
        LOG_MSG(log_level,                                              \
                "%s.%lx.%lx v=%d crc=%x cs=%ld pt=%ld %s msg-id=%lx "fmt, \
                __uuid_str,                                             \
                RAFT_NET_CLIENT_USER_ID_2_UINT64(pmdb_rncui, 0, 2),     \
                RAFT_NET_CLIENT_USER_ID_2_UINT64(pmdb_rncui, 0, 3),     \
                (pmdb_obj)->pmdb_obj_version,                           \
                (pmdb_obj)->pmdb_obj_crc,                               \
                (pmdb_obj)->pmdb_obj_commit_seqno,                      \
                (pmdb_obj)->pmdb_obj_pending_term,                      \
                inet_ntoa((pmdb_obj)->pmdb_obj_remote_addr.sin_addr),   \
                (pmdb_obj)->pmdb_obj_msg_id,                            \
                ##__VA_ARGS__);                                         \
    }

#define PMDB_STR_DEBUG(log_level, pmdb_rncui, fmt, ...)                 \
    {                                                                   \
        char __uuid_str[UUID_STR_LEN];                                  \
        uuid_unparse((pmdb_rncui)->rncui_key.v0.rncui_v0_uuid[0],       \
                     __uuid_str);                                       \
        LOG_MSG(log_level, "%s.%lx.%lx: "fmt,                           \
                __uuid_str,                                             \
                (pmdb_rncui)->rncui_key.v0.rncui_v0_uint64[2],          \
                (pmdb_rncui)->rncui_key.v0.rncui_v0_uint64[3],          \
                ##__VA_ARGS__);                                         \
    }                                                                   \

static void
pmdb_obj_crc(struct pmdb_object *obj)
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
                 int64_t current_raft_term)
{
    NIOVA_ASSERT(pmdb_obj);

    memset(pmdb_obj, 0, sizeof(*pmdb_obj));

    pmdb_obj->pmdb_obj_version = version;
    pmdb_obj->pmdb_obj_commit_seqno = ID_ANY_64bit;
    pmdb_obj->pmdb_obj_pending_term = current_raft_term;
}

static rocksdb_t *
pmdb_get_rocksdb_instance(void)
{
    rocksdb_t *db = raft_server_get_rocksdb_instance(raft_net_get_instance());
    NIOVA_ASSERT(db);

    return db;
}

static rocksdb_readoptions_t *
pmdb_get_rocksdb_readopts(void)
{
    NIOVA_ASSERT(pmdbRocksdbReadOpts)
    return pmdbRocksdbReadOpts;
}

static int
pmdb_init_rocksdb(void)
{
    if (pmdbRocksdbCFH)
        return 0;

    pmdbRocksdbReadOpts = rocksdb_readoptions_create();
    if (!pmdbRocksdbReadOpts)
        return -ENOMEM;

    rocksdb_options_t *opts = rocksdb_options_create();
    if (!opts)
    {
        rocksdb_readoptions_destroy(pmdbRocksdbReadOpts);
        pmdbRocksdbReadOpts = NULL;
        return -ENOMEM;
    }

    char *err = NULL;
    rocksdb_options_set_create_if_missing(opts, 1);

    pmdbRocksdbCFH =
        rocksdb_create_column_family(pmdb_get_rocksdb_instance(), opts,
                                     PMDB_COLUMN_FAMILY_NAME, &err);

    rocksdb_options_destroy(opts);

    if (err)
    {
        rocksdb_readoptions_destroy(pmdbRocksdbReadOpts);
        pmdbRocksdbReadOpts = NULL;

        pmdbRocksdbCFH = NULL;

        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_create_column_family(): %s",
                       err);
    }

    return err ? -EINVAL : 0;
}

static rocksdb_column_family_handle_t *
pmdb_get_rocksdb_column_family_handle(void)
{
    if (!pmdbRocksdbCFH)
    {
        int rc = pmdb_init_rocksdb();
        if (rc)
            return NULL;
    }

    return pmdbRocksdbCFH;
}

void
pmdb_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct pmdb_rpc_msg) <=
                        PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP);

    // enum PmdbOpType must fit into 'uint8_t pmdbrm_op'
    COMPILE_TIME_ASSERT(pmdb_op_any < (1 << sizeof(uint8_t)) * NBBY);
}

#define PMDB_ARG_CHECK(op, rncr)                                        \
    NIOVA_ASSERT(                                                       \
        (rncr) &&                                                       \
        (rncr)->rncr_type == op &&                                      \
        ((rncr)->rncr_request ||                                        \
         op == RAFT_NET_CLIENT_REQ_TYPE_COMMIT) &&                      \
        (rncr)->rncr_reply &&                                           \
        (rncr)->rncr_request_or_commit_data &&                          \
        ((rncr)->rncr_request_or_commit_data_size >=                    \
         sizeof(struct pmdb_rpc_msg)) &&                                \
        (((char *)(rncr)->rncr_request ==                               \
          (rncr)->rncr_request_or_commit_data) ||                       \
         op == RAFT_NET_CLIENT_REQ_TYPE_COMMIT) &&                      \
        (rncr)->rncr_reply_data_max_size >= sizeof(struct pmdb_rpc_msg))

#define PMDB_CFH_MUST_GET()                             \
({                                                      \
    rocksdb_column_family_handle_t *cfh =               \
        pmdb_get_rocksdb_column_family_handle();        \
                                                        \
    NIOVA_ASSERT(cfh);                                  \
    cfh;                                                \
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

    char *get_value =
        rocksdb_get_cf(pmdb_get_rocksdb_instance(),
                       pmdb_get_rocksdb_readopts(), PMDB_CFH_MUST_GET(),
                       PMDB_RNCUI_2_KEY(rncui), PMDB_ENTRY_KEY_LEN,
                       &val_len, &err);

    PMDB_STR_DEBUG(LL_NOTIFY, rncui, "err=%s val=%p", err, get_value);

    if (err || !get_value)
        return rc; //Xxx need a proper error code intepreter

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

    if (obj->pmdb_obj_pending_term >= current_raft_term)
        rc = -EOVERFLOW;

    PMDB_OBJ_DEBUG((rc ? LL_WARN : LL_DEBUG), rncui, obj, "")

    return rc;
}

static void
pmdb_obj_to_reply(const struct pmdb_object *obj, struct pmdb_rpc_msg *reply,
                  const int64_t current_raft_term)
{
    NIOVA_ASSERT(obj && reply);

    reply->pmdbrm_write_seqno = obj->pmdb_obj_commit_seqno;

    reply->pmdbrm_write_pending =
        obj->pmdb_obj_pending_term == current_raft_term ? 1 : 0;
}

/**
 * pmdb_sm_handler_client_lookup - perform a key lookup in the PMDB column-
 *    family.
 * RETURN: 0 is always returned so that a reply will be delivered to the
 *    client.
 */
static int
pmdb_sm_handler_client_lookup(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct pmdb_rpc_msg *pmdb_req =
         (const struct pmdb_rpc_msg *)rncr->rncr_request_or_commit_data;

    struct pmdb_object pmdb_obj = {0};

    int rc = pmdb_object_lookup(&pmdb_req->pmdbrm_user_id, &pmdb_obj,
                                rncr->rncr_current_term);

    if (!rc)
        pmdb_obj_to_reply(&pmdb_obj,
                          RAFT_NET_MAP_RPC(pmdb_rpc_msg, rncr->rncr_reply),
                          rncr->rncr_current_term);
    else
        raft_client_msg_error_set(rncr->rncr_reply, rc, 0);

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
    obj->pmdb_obj_version = pmdb_get_current_version();
    obj->pmdb_obj_pending_term = current_term;

    pmdb_obj_crc(obj);
}

static void
pmdb_prep_obj_write(struct raft_net_sm_write_supplements *ws,
                    const struct raft_net_client_user_id *rncui,
                    struct pmdb_object *obj, const int64_t term)
{
    NIOVA_ASSERT(ws && obj);

    pmdb_prep_raft_entry_write_obj(obj, term);

    raft_net_sm_write_supplement_add(
        ws, (void *)pmdb_get_rocksdb_column_family_handle(),
        NULL /* no callback needed yet */, PMDB_RNCUI_2_KEY(rncui),
        PMDB_ENTRY_KEY_LEN, (const char *)obj, sizeof(*obj));
}

static void
pmdb_prep_raft_entry_write(struct raft_net_client_request *rncr,
                           struct pmdb_object *obj)
{
    NIOVA_ASSERT(rncr && obj);

    const struct pmdb_rpc_msg *pmdb_req =
         (const struct pmdb_rpc_msg *)rncr->rncr_request_or_commit_data;

    rncr->rncr_write_raft_entry = true;

    // Mark that the object is pending a write in this leader's term.
    pmdb_prep_obj_write(&rncr->rncr_sm_write_supp, &pmdb_req->pmdbrm_user_id,
                        obj, rncr->rncr_current_term);

    PMDB_OBJ_DEBUG(LL_DEBUG, &pmdb_req->pmdbrm_user_id, obj, "");
}

static void
pmdb_prep_sm_apply_write(struct raft_net_client_request *rncr,
                         struct pmdb_object *obj)
{
    NIOVA_ASSERT(rncr && obj);

    const struct pmdb_rpc_msg *pmdb_req =
         (const struct pmdb_rpc_msg *)rncr->rncr_request_or_commit_data;

    // Reset the pending term value with -1
    pmdb_prep_obj_write(&rncr->rncr_sm_write_supp, &pmdb_req->pmdbrm_user_id,
                        obj, ID_ANY_64bit);

    PMDB_OBJ_DEBUG(LL_DEBUG, &pmdb_req->pmdbrm_user_id, obj, "");
}

/**
 * pmdb_sm_handler_client_write - lookup the object and ensure that the
 *    requested write sequence number is consistent with the pmdb-object.
 *
 * RETURN:  Returning without an error and with rncr_write_raft_entry=false
 *    will cause an immediate reply to the client.  Returning any non-zero
 *    value causes the request to terminate immediately without any reply being
 *    issued.
 */
static int
pmdb_sm_handler_client_write(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct pmdb_rpc_msg *pmdb_req =
        (const struct pmdb_rpc_msg *)rncr->rncr_request_or_commit_data;

    struct pmdb_object obj = {0};

    int rc = pmdb_object_lookup(&pmdb_req->pmdbrm_user_id, &obj,
                                rncr->rncr_current_term);
    if (rc)
    {
        if (rc == -ENOENT)
        {
            pmdb_object_init(&obj, pmdb_get_current_version(),
                             rncr->rncr_current_term);
            rc = 0;
        }
        else
        {
            PMDB_STR_DEBUG(LL_NOTIFY, &pmdb_req->pmdbrm_user_id,
                           "pmdb_object_lookup(): %s", strerror(-rc));

            /* This appears to be a system error.  Mark it and reply to the
             * client.
             */
            raft_client_net_request_error_set(rncr, rc, rc, 0);

            return 0;
        }
    }

    // Check if the request was already committed and applied
    if (pmdb_req->pmdbrm_write_seqno <= obj.pmdb_obj_commit_seqno)
    {
        raft_client_net_request_error_set(rncr, -EALREADY, 0, 0);
    }

    else if (pmdb_req->pmdbrm_write_seqno == (obj.pmdb_obj_commit_seqno + 1))
    {
        /* Check if request has already been placed into the log but not yet
         * applied.
         */
        if (obj.pmdb_obj_pending_term == rncr->rncr_current_term)
            raft_client_net_request_error_set(rncr, -EINPROGRESS, 0,
                                              -EINPROGRESS);

        else // Request sequence test passes, request will enter the raft log.
            pmdb_prep_raft_entry_write(rncr, &obj);
    }

    else // Request sequence is too far ahead
    {
        raft_client_net_request_error_set(rncr, -EBADE, 0, -EBADE);
    }

    PMDB_OBJ_DEBUG((rncr->rncr_op_error == -EBADE ? LL_NOTIFY : LL_DEBUG),
                   &pmdb_req->pmdbrm_user_id, &obj, "op-err=%s",
                   strerror(rncr->rncr_op_error));

    return 0;
}

static int
pmdb_sm_handler_client_read(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_rpc_msg *pmdb_req =
        (const struct pmdb_rpc_msg *)req->rcrm_data;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    struct pmdb_rpc_msg *pmdb_reply = (struct pmdb_rpc_msg *)reply->rcrm_data;

    NIOVA_ASSERT(pmdb_req->pmdbrm_data_size <= PMDB_MAX_APP_RPC_PAYLOAD_SIZE);

    const size_t max_reply_size =
        rncr->rncr_reply_data_max_size - PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP;

    const ssize_t rrc =
        pmdbApi->pmdb_read(&pmdb_req->pmdbrm_user_id, pmdb_req->pmdbrm_data,
                           pmdb_req->pmdbrm_data_size, pmdb_reply->pmdbrm_data,
                           max_reply_size);

    if (rrc < 0)
    {
        pmdb_reply->pmdbrm_data_size = 0;
        raft_client_net_request_error_set(rncr, rrc, rrc, rrc);

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, &rncr->rncr_remote_addr,
                            "pmdbApi::read(): %s", strerror(rrc));
    }
    else if (rrc > (ssize_t)max_reply_size)
    {
        raft_client_net_request_error_set(rncr, -E2BIG, 0, -E2BIG);
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, &rncr->rncr_remote_addr,
                            "pmdbApi::read(): reply too large (%zd)", rrc);
    }
    else
    {
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_DEBUG, req, &rncr->rncr_remote_addr,
                            "pmdbApi::read(): reply-size=%zd", rrc);
    }

    return 0;
}

static void
pmdb_reply_init(const struct pmdb_rpc_msg *req, struct pmdb_rpc_msg *reply)
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
pmdb_sm_handler_client_rw_op(struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE &&
                 rncr->rncr_request && rncr->rncr_reply &&
                 rncr->rncr_request_or_commit_data_size >=
                 sizeof(struct pmdb_rpc_msg));

    const struct pmdb_rpc_msg *pmdb_req =
        (const struct pmdb_rpc_msg *)rncr->rncr_request_or_commit_data;

    const enum PmdbOpType op = pmdb_req->pmdbrm_op;

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, rncr->rncr_request, &rncr->rncr_remote_addr,
                        "op=%u", op);

    switch (op)
    {
    case pmdb_op_noop:
        return 0; // noop should be harmless

    case pmdb_op_read:
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
 * pmdb_sm_handler_pmdb_sm_apply - ri_server_sm_request apply cb for pumiceDB.
 *   This function has 2 primary roles:  1) updating (if leader), or creating
 *   (if follower), the pmdb_object for this request.  2) calling into the
 *   pmdb app layer to obtain any KVs that it would like to have written.
 *   The underlying raft layer, via raft_server_state_machine_apply(), is
 *   reponsible for writing these KVs into rocksDB through the function,
 *   raft_server_sm_apply_opt().
 */
static raft_server_sm_apply_cb_t
pmdb_sm_handler_pmdb_sm_apply(const struct pmdb_rpc_msg *pmdb_req,
                              struct raft_net_client_request *rncr)
{
    if (!pmdb_req || !rncr)
        return;

    const struct raft_net_client_user_id *rncui = &pmdb_req->pmdbrm_user_id;

    struct pmdb_object obj = {0};

    int rc = pmdb_object_lookup(rncui, &obj, rncr->rncr_current_term);

    if (rc)
    {
        if (raft_client_net_request_instance_is_leader(rncr))
        {
            // A failure here means that somehow the DB has become "corrupted".
            /* Xxx This is probably too aggressive and we should allow for an
             *     error to be passed into pmdb_apply() and returned to the
             *     client.
             */
            PMDB_OBJ_DEBUG(LL_FATAL, rncui, &obj, "pmdb_object_lookup(): %s",
                           strerror(-rc));
        }

        PMDB_OBJ_DEBUG(((rc == -ENOENT) ? LL_DEBUG : LL_WARN), rncui, &obj,
                       "pmdb_object_lookup(): %s", strerror(-rc));

        rc = 0;
    }

    // The object receiving the apply must have its pending_term value reset.
    pmdb_prep_sm_apply_write(rncr, &obj);

    struct raft_net_sm_write_supplements *ws = &rncr->rncr_sm_write_supp;
    struct pmdb_apply_handle pah = {.pah_rncui = rncui, .pah_ws = ws};

    // Call into the application so it may emplace its own KVs.
    pmdbApi->pmdb_apply(rncui, pmdb_req->pmdbrm_data,
                        pmdb_req->pmdbrm_data_size, (void *)&pah);
}

static int
pmdb_sm_handler_pmdb_req_check(const struct pmdb_rpc_msg *pmdb_req)
{
    if (pmdb_req->pmdbrm_data_size > PMDB_MAX_APP_RPC_PAYLOAD_SIZE)
        return -EINVAL;

    return 0;
}

static int
pmdb_sm_handler(struct raft_net_client_request *rncr)
{
    if (!rncr || !rncr->rncr_request)
        return -EINVAL;

    else if (rncr->rncr_request_or_commit_data_size <
             sizeof(struct pmdb_rpc_msg))
        return -EBADMSG;

    const struct pmdb_rpc_msg *pmdb_req =
        (const struct pmdb_rpc_msg *)rncr->rncr_request_or_commit_data;

    if (pmdb_net_calc_rpc_msg_size(pmdb_req) !=
        rncr->rncr_request_or_commit_data_size)
        return -EBADMSG;

    /* Copy some content from the request to the reply for the individual
     * handlers.
     */
    pmdb_reply_init(pmdb_req,
                    RAFT_NET_MAP_RPC(pmdb_rpc_msg, rncr->rncr_reply));

    /* Initialize this value here.  Write requests that wish to be placed into
     * raft log will set this to true.
     */
    rncr->rncr_write_raft_entry = false;

    int rc = 0;

    switch (rncr->rncr_type)
    {
    case RAFT_NET_CLIENT_REQ_TYPE_READ:  // fall through
    case RAFT_NET_CLIENT_REQ_TYPE_WRITE:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_NONE; // fall through

    case RAFT_NET_CLIENT_REQ_TYPE_NONE:
    {
        if (rncr->rncr_reply_data_max_size < sizeof(struct pmdb_rpc_msg))
            return -ENOSPC;

        rc = pmdb_sm_handler_pmdb_req_check(pmdb_req);
        if (rc)
        {
            raft_client_net_request_error_set(rncr, rc, 0, rc);

            // There's a problem with the application RPC request
            DBG_RAFT_CLIENT_RPC(LL_NOTIFY, rncr->rncr_request,
                                &rncr->rncr_remote_addr,
                                "pmdb_sm_handler_pmdb_req_check(): %s",
                                strerror(-rc));
            return 0;
        }

        return pmdb_sm_handler_client_rw_op(rncr);
    }

    case RAFT_NET_CLIENT_REQ_TYPE_COMMIT:
        pmdb_sm_handler_pmdb_sm_apply(pmdb_req, rncr);
        return 0;

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
int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api)
{
    pmdbApi = pmdb_api;

    if (!raft_uuid_str || !raft_instance_uuid_str || !pmdb_api ||
        !pmdb_api->pmdb_apply || !pmdb_api->pmdb_read)
        return -EINVAL;

    return raft_net_server_instance_run(raft_uuid_str, raft_instance_uuid_str,
                                        pmdb_sm_handler,
                                        RAFT_INSTANCE_STORE_ROCKSDB);
}

/**
 * PmdbClose - called from application context to shutdown the pumicedb exec
 *   thread.
 */
int
PmdbClose(void)
{
    rocksdb_column_family_handle_destroy(
        pmdb_get_rocksdb_column_family_handle());

    return raft_server_instance_shutdown(raft_net_get_instance());
}

rocksdb_t *
PmdbGetRocksDB(void)
{
    return pmdb_get_rocksdb_instance();
}
