/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <errno.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <uuid/uuid.h>

#include "alloc.h"
#include "crc32.h"
#include "log.h"
#include "pumice_db_net.h"
#include "pumice_db_client.h"
#include "registry.h"

#define PMDB_MIN_REQUEST_TIMEOUT_SECS 10
static unsigned long pmdbClientDefaultTimeoutSecs =
    PMDB_MIN_REQUEST_TIMEOUT_SECS;

struct pmdb_client_request
{
    pmdb_obj_id_t        pcreq_obj_id;
    struct pmdb_msg      pcreq_msg_request;
    struct pmdb_msg      pcreq_msg_reply;
    enum PmdbOpType      pcreq_op;
    const char          *pcreq_user_request;
    const size_t         pcreq_user_request_size;
    char                *pcreq_user_reply;
    const size_t         pcreq_user_reply_size;
    off_t                pcreq_user_reply_offset;
    struct pmdb_obj_stat pcreq_stat;
    struct timespec      pcreq_timeout;
    void               (*pcreq_user_cb)(void *, ssize_t /*status & reply size*/);
    void                *pcreq_user_arg;
};

static void
pmdb_client_request_lookup_completion(struct pmdb_client_request *pcreq,
                                      ssize_t status)
{
    NIOVA_ASSERT(pcreq && pcreq->pcreq_op == pmdb_op_lookup);

    ssize_t ret_status;

    if (status < 0)
        ret_status = status;

    else if (status != sizeof(struct pmdb_msg))
        ret_status = -EMSGSIZE;

    else if (pcreq->pcreq_user_reply_size != sizeof(struct pmdb_obj_stat))
        ret_status = -EINVAL;

    else
    {
        ret_status = 0;

        struct pmdb_obj_stat *pst =
            (struct pmdb_obj_stat *)pcreq->pcreq_user_reply;

        pst->obj_id = pcreq->pcreq_obj_id;
        pst->write_sequence_num = reply->pcreq_msg_reply.pmdbrm_write_seqno;
        pst->write_op_pending = !!reply->pcreq_msg_reply.pmdbrm_write_pending;
    }

    /* This function may be called from blocking request context as well.
     * In that case, there will be no user cb.
     */
    if (pcreq->pcreq_user_cb)
        pcreq->pcreq_user_cb(pcreq->pcreq_user_arg, ret_status);
}

static void
pmdb_client_request_cb(void *arg, ssize_t status)
{
    NIOVA_ASSERT(arg);

    struct pmdb_client_request *pcreq = (struct pmdb_client_request *)arg;

    switch (pcreq->pcreq_op)
    {
    case pmdb_op_lookup:
        pmdb_client_request_lookup_completion(pcreq, status);
        break;
    case pmdb_op_read:
    case pmdb_op_write:
        pmdb_client_request_rw_cb(pcreq, status);
        break;
    default:
        break;
    }

    // Release the pcreq which was allocated in pmdb_client_request_new()
    niova_free(pcreq);
}

/**
 * pmdb_client_request_init - internal client request initialization.
 * @pcreq - pointer to the client request
 * @obj_id - object indentifier of the request
 * @op - operation type which is copied into the rpc msg and the pcreq.
 * @req_buf - additional user request data which is to be appended in the
 *   pmdbrm_data section of the PmdbMsg_t
 * @req_buf_size - size of the req_buf
 * @reply_buf - user reply data buffer, this data which may arrive in the
 *   PmdbMsg_t reply message attached to pmdbrm_data.  Reply_buf may also be
 *   used for reply context which are not bulk oriented, such as elements
 *   contained directly in the reply PmdbMsg_t.  This is the case for the
 *   pmdb_op_lookup command.
 * @ts - timeout
 * @user_cb - user callback to be issued on completion or timeout.
 * @user_arg - user argument which is supplied as the first parameter to the
 *   user_cb.
 */
static struct pmdb_client_request *
pmdb_client_request_new(const pmdb_obj_id_t *obj_id,
                        enum PmdbOpType op, const char *req_buf,
                        const size_t req_buf_size, char *reply_buf,
                        const size_t reply_buf_size,
                        const struct timespec ts,
                        void (*user_cb)(void *, ssize_t),
                        void *user_arg, int *status)
{
    if (!obj_id)
    {
        if (status)
            *status = -EINVAL;
        return NULL;
    }

    struct pmdb_client_request *pcreq =
        niova_malloc_can_fail(sizeof(struct pmdb_client_request));

    if (!pcreq)
    {
        if (status)
            *status = -ENOMEM;
        return NULL;
    }

    // Convert obj_id to to rncui
    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    // Initialize client request
    memset(pcreq, 0, sizeof(struct pmdb_client_request));

    pcreq->pcreq_msg_request.pmdbrm_magic = PMDB_MSG_MAGIC;
    raft_net_client_user_id_copy(&pcreq->pcreq_msg_request.pmdbrm_user_id,
                                 &rncui);

    NIOVA_CRC_OBJ(&pcreq->pcreq_msg_request, struct pmdb_msg, pmdbrm_crc, 0);

    pcreq->pcreq_op = op;
    pcreq->pcreq_user_request = req_buf;
    pcreq->pcreq_user_request_size = req_buf_size;

    pcreq->pcreq_user_reply = reply_buf;
    pcreq->pcreq_user_reply_size = reply_buf_size;

    pcreq->pcreq_timeout = timeout;
    pcreq->pcreq_user_cb = user_cb;
    pcreq->pcreq_user_arg = user_arg;

    return pcreq;
}

static int
pmdb_obj_lookup_internal(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                         struct pmdb_obj_stat *user_stat, const bool blocking,
                         const struct timespec timeout,
                         void (*user_cb)(void *, ssize_t), void *user_arg)
{
    if (!pmdb || !obj_id || !user_stat || (blocking && !cb))
        return -EINVAL;

    int rc = 0;

    struct pmdb_client_request *pcreq =
        pmdb_client_request_new(obj_id, pmdb_op_lookup, NULL, 0,
                                (void *)user_stat,
                                sizeof(struct pmdb_obj_stat),
                                timeout, user_cb, user_arg, &rc);
    if (!pcreq)
        return rc;

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    // XXX this needs to be converted to IOVs
    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui,
                                      &pcreq->pcreq_msg_request,
                                      sizeof(struct pmdb_msg),
                                      &pcreq->pcreq_msg_reply,
                                      sizeof(struct pmdb_msg),
                                      timeout, blocking,
                                      pmdb_client_request_cb, pcreq);
}

/**
 * PmdbObjLookup - blocking object lookup public routine.
 */
int
PmdbObjLookup(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
              pmdb_obj_stat_t *ret_stat)
{
    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    return pmdb_obj_lookup_internal(pmdb, obj_id, ret_stat, true, timeout,
                                    NULL, NULL);
}

/**
 * PmdbObjLookupNB - non-blocking public lookup routine.
 */
int
PmdbObjLookupNB(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                struct pmdb_obj_stat *ret_stat, void (*cb)(void *, ssize_t),
                void *arg)
{
    if (!cb)
        return -EINVAL;

    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    return pmdb_obj_lookup_internal(pmdb, obj_id, ret_stat, false, timeout, cb,
                                    arg);
}

/**
 * PmdbObjGetNB - non-blocking public read call.
 */
int
PmdbObjGetNB(pmdb_t pmdb, const pmdb_obj_id_t *obj_id, char *buf,
             size_t buf_size, void (*cb)(void *, ssize_t), void *arg)
{
    if (!cb)
        return -EINVAL;

    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    return pmdb_obj_lookup_internal(pmdb, obj_id, ret_stat, false, timeout, cb,
                                    arg);
}

static int
pmdb_obj_put_internal(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                      const char *user_buf, size_t user_buf_size,
                      const bool blocking, const struct timespec timeout,
                      void (*user_cb)(void *, ssize_t), void *user_arg)
{
    // NULL user_buf or buf_size of 0 is OK
    if (!pmdb || !obj_id || (blocking && !cb))
        return -EINVAL;

    int rc = 0;

    struct pmdb_client_request *pcreq =
        pmdb_client_request_new(obj_id, pmdb_op_write, user_buf, user_buf_size,
                                NULL, 0, timeout, user_cb, user_arg, &rc);
    if (!pcreq)
        return rc;

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    // XXX this needs to be converted to IOVs
    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui,
                                      &pcreq->pcreq_msg_request,
                                      sizeof(struct pmdb_msg),
                                      &pcreq->pcreq_msg_reply,
                                      sizeof(struct pmdb_msg),
                                      timeout, blocking,
                                      pmdb_client_request_cb, pcreq);
}


/**
 * PmdbObjLookupNB - non-blocking public lookup routine.
 */
int
PmdbObjPutNB(pmdb_t pmdb, const pmdb_obj_id_t *obj_id, const char *buf,
             size_t buf_size, void (*cb)(void *, ssize_t), void *arg)
{
    if (!cb)
        return -EINVAL;

    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    return pmdb_obj_lookup_internal(pmdb, obj_id, ret_stat, false, timeout, cb,
                                    arg);
}

static int
pmdb_obj_id_cb(const char *data, const size_t data_size,
               struct raft_net_client_user_id *out_rncui)
{
    if (!data || data_size < sizeof(struct pmdb_msg) || !out_rncui)
        return -EINVAL;

    const struct pmdb_msg *msg = (const struct pmdb_msg *)data;

    raft_net_client_user_id_copy(out_rncui, &msg->pmdbrm_user_id);

    return 0;
}

pmdb_t
PmdbClientStart(const char *raft_uuid_str, const char *raft_client_uuid_str)
{
    if (!raft_uuid_str || !raft_client_uuid_str)
    {
        errno = -EINVAL;
        return NULL;
    }

    pmdb_t pmdb = NULL;

    int rc = raft_client_init(raft_uuid_str, raft_client_uuid_str,
                              pmdb_obj_id_cb, &pmdb);
    if (rc)
    {
        errno = -rc;
        return NULL;
    }

    NIOVA_ASSERT(pmdb);

    return pmdb;
}
