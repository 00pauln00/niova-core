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

REGISTRY_ENTRY_FILE_GENERATE;

struct pmdb_client_request
{
    pmdb_obj_id_t          pcreq_obj_id;
    enum PmdbOpType        pcreq_op;
    const void            *pcreq_user_request;
    const size_t           pcreq_user_request_size;
    void                  *pcreq_user_reply;
    const size_t           pcreq_user_reply_size;
    off_t                  pcreq_user_reply_offset;
    struct pmdb_obj_stat  *pcreq_user_pmdb_stat;
    raft_net_request_tag_t pcreq_tag;
    struct timespec        pcreq_timeout;
    pmdb_user_cb_t         pcreq_user_cb;
    void                  *pcreq_user_arg;
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-variable-sized-type-not-at-end"
#endif
    struct pmdb_msg        pcreq_msg_request;
    struct pmdb_msg        pcreq_msg_reply;
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
};

static void
pmdb_client_completion_fill_pmdb_stat(struct pmdb_client_request *pcreq,
                                      ssize_t status, void *reply_buff)
{
    NIOVA_ASSERT(pcreq);

    if (pcreq->pcreq_user_pmdb_stat)
    {
        pmdb_obj_stat_t *pst = pcreq->pcreq_user_pmdb_stat;
        const struct pmdb_msg *reply = &pcreq->pcreq_msg_reply;

        pst->status = status;
        pst->obj_id = pcreq->pcreq_obj_id;
        pst->sequence_num = reply->pmdbrm_write_seqno;
        pst->write_op_pending = !!reply->pmdbrm_write_pending;
        pst->reply_size = reply->pmdbrm_data_size;
        pst->reply_buffer = reply_buff;
    }
}

static void
pmdb_client_request_lookup_completion(struct pmdb_client_request *pcreq,
                                      ssize_t status)
{
    NIOVA_ASSERT(pcreq && pcreq->pcreq_op == pmdb_op_lookup);

    ssize_t ret_status;

    if (status < 0)
    {
        ret_status = status;
    }
    else if (!pcreq->pcreq_user_pmdb_stat)
    {
        ret_status = -EINVAL;
    }
    else
    {
        // Copy the error from the pmdb msg
        ret_status = pcreq->pcreq_msg_reply.pmdbrm_err;

        pmdb_client_completion_fill_pmdb_stat(pcreq, ret_status, NULL);
    }

    /* This function may be called from blocking request context as well.
     * In that case, there will be no user cb.
     */
    if (pcreq->pcreq_user_cb)
        pcreq->pcreq_user_cb(pcreq->pcreq_user_arg, ret_status);
}

static void
pmdb_client_request_rw_completion(struct pmdb_client_request *pcreq,
                                  ssize_t status, void *reply_buff)
{
    NIOVA_ASSERT(pcreq && (pcreq->pcreq_op == pmdb_op_write ||
                           pcreq->pcreq_op == pmdb_op_read));

    pmdb_client_completion_fill_pmdb_stat(pcreq, status, reply_buff);

    if (pcreq->pcreq_user_cb)
        pcreq->pcreq_user_cb(pcreq->pcreq_user_arg, status);
}

/**
 * pmdb_client_request_cb - called from 'sa' destructor context.
 * @arg:  opaque pointer to pmdb_client_request
 * @status:  operation status.  A positive value represents the amount of data
 *    copied into our reply buffer.  Otherwise, it's the error code.
 * NOTE:  the 'status' may be translated to a "system" or "app" error code
 *    prior to calling the application callback.  A positive code is an
 *    application error - negative is for system.
 */
static void
pmdb_client_request_cb(void *arg, ssize_t status, void *reply_buff)
{
    NIOVA_ASSERT(arg);

    struct pmdb_client_request *pcreq = (struct pmdb_client_request *)arg;

    if (status > 0) // status represents reply data size
    {
        const struct pmdb_msg *reply = &pcreq->pcreq_msg_reply;

        // Incorrect size is translated to be a system error.
        if (status != (sizeof(struct pmdb_msg) + reply->pmdbrm_data_size))
            status = -EMSGSIZE;

        else if (reply->pmdbrm_err)
            status = ABS(reply->pmdbrm_err);

        else
            status = 0; // success
    }

    switch (pcreq->pcreq_op)
    {
    case pmdb_op_lookup:
        pmdb_client_request_lookup_completion(pcreq, status);
        break;
    case pmdb_op_read:
    case pmdb_op_write:
        pmdb_client_request_rw_completion(pcreq, status, reply_buff);
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
                        enum PmdbOpType op, const void *req_buf,
                        const size_t req_buf_size, void *reply_buf,
                        const size_t reply_buf_size,
                        struct pmdb_obj_stat *user_pmdb_stat,
                        const struct timespec ts,
                        pmdb_user_cb_t user_cb, void *user_arg, int *status)
{
    if (!obj_id ||
        ((op == pmdb_op_write || op == pmdb_op_lookup) && !user_pmdb_stat))
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
    pcreq->pcreq_msg_request.pmdbrm_op = op;
    pcreq->pcreq_msg_request.pmdbrm_data_size = req_buf_size;
    if (op == pmdb_op_write)
        pcreq->pcreq_msg_request.pmdbrm_write_seqno =
            user_pmdb_stat->sequence_num;

    raft_net_client_user_id_copy(&pcreq->pcreq_msg_request.pmdbrm_user_id,
                                 &rncui);

    NIOVA_CRC_OBJ(&pcreq->pcreq_msg_request, struct pmdb_msg, pmdbrm_crc, 0);

    pcreq->pcreq_op = op;
    pcreq->pcreq_user_request = req_buf;
    CONST_OVERRIDE(size_t, pcreq->pcreq_user_request_size, req_buf_size);

    pcreq->pcreq_user_reply = reply_buf;
    pcreq->pcreq_obj_id = *obj_id;


    CONST_OVERRIDE(size_t, pcreq->pcreq_user_reply_size, reply_buf_size);

    /* Allow the application to provide a tag through the provided user_stat
     * structure.  This is not a general purpose "feature" - it is intended for
     * testing.
     */
    pcreq->pcreq_tag =
        (raft_net_request_tag_t)(user_pmdb_stat ?
                                 user_pmdb_stat->status :
                                 RAFT_NET_TAG_NONE);

    pcreq->pcreq_user_pmdb_stat = user_pmdb_stat;

    pcreq->pcreq_timeout = ts;
    pcreq->pcreq_user_cb = user_cb;
    pcreq->pcreq_user_arg = user_arg;

    return pcreq;
}

static void
pmdb_check_and_set_default_timeout(pmdb_request_opts_t *pmdb_req_opt)
{
    if (pmdb_req_opt->pro_timeout.tv_sec == 0 &&
        pmdb_req_opt->pro_timeout.tv_nsec == 0)
    {
        struct timespec timeout = {pmdb_get_default_request_timeout(), 0};
        CONST_OVERRIDE(struct timespec, pmdb_req_opt->pro_timeout, timeout);
    }
}

static int
pmdb_obj_lookup_internal(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                         pmdb_request_opts_t *pmdb_req_opt)
{
    if (!pmdb || !obj_id || !pmdb_req_opt->pro_stat ||
        (pmdb_req_opt->pro_non_blocking && !pmdb_req_opt->pro_non_blocking_cb))
        return -EINVAL;

    int rc = 0;

    struct pmdb_client_request *pcreq =
        pmdb_client_request_new(obj_id, pmdb_op_lookup, NULL, 0, NULL, 0,
                                pmdb_req_opt->pro_stat,
                                pmdb_req_opt->pro_timeout,
                                pmdb_req_opt->pro_non_blocking_cb,
                                pmdb_req_opt->pro_arg, &rc);
    if (!pcreq)
        return rc;

    struct iovec req_iov = {
        .iov_base = &pcreq->pcreq_msg_request,
        .iov_len = sizeof(struct pmdb_msg),
    };

    struct iovec reply_iov = {
        .iov_base = &pcreq->pcreq_msg_reply,
        .iov_len = sizeof(struct pmdb_msg),
    };

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    int opts = RCRT_READ;
    if (pmdb_req_opt->pro_non_blocking)
        opts |= RCRT_NON_BLOCKING;

    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui, &req_iov, 1,
                                      &reply_iov, 1, false,
                                      pmdb_req_opt->pro_timeout,
                                      opts,
                                      pmdb_client_request_cb, pcreq,
                                      pcreq->pcreq_tag);
}

/**
 * PmdbObjLookup - blocking object lookup public routine.
 */
int
PmdbObjLookup(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
              pmdb_obj_stat_t *ret_stat)
{
    /* Initialize the request options */
    pmdb_request_opts_t pmdb_req_opt;
    pmdb_request_options_init(&pmdb_req_opt, 1, 0, ret_stat, NULL, NULL, NULL,
                              0, pmdb_get_default_request_timeout());

    return pmdb_obj_lookup_internal(pmdb, obj_id, &pmdb_req_opt);
}

/**
 * PmdbObjLookupX - public lookup routine (Blocking or Non-blocking).
 */
int
PmdbObjLookupX(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                pmdb_request_opts_t *pmdb_req_opt)
{
    if (!pmdb_req_opt || (pmdb_req_opt->pro_non_blocking &&
                          !pmdb_req_opt->pro_non_blocking_cb))
        return -EINVAL;

    /* Use default timeout if not set by user. */
    pmdb_check_and_set_default_timeout(pmdb_req_opt);

    return pmdb_obj_lookup_internal(pmdb, obj_id, pmdb_req_opt);
}

static int
pmdb_obj_put_internal(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                      const char *user_buf, size_t user_buf_size,
                      pmdb_request_opts_t *pmdb_req_opt)
{
    // NULL user_buf or buf_size of 0 is OK
    if (!pmdb || !pmdb_req_opt->pro_stat || !obj_id ||
        (pmdb_req_opt->pro_non_blocking && !pmdb_req_opt->pro_non_blocking_cb))
        return -EINVAL;

    int rc = 0;

    struct pmdb_client_request *pcreq =
        pmdb_client_request_new(obj_id, pmdb_op_write, user_buf, user_buf_size,
                                NULL, 0, pmdb_req_opt->pro_stat,
                                pmdb_req_opt->pro_timeout,
                                pmdb_req_opt->pro_non_blocking_cb,
                                pmdb_req_opt->pro_arg, &rc);
    if (!pcreq)
        return rc;

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    struct iovec req_iovs[2] = {
        [0].iov_base = (void *)&pcreq->pcreq_msg_request,
        [0].iov_len = sizeof(struct pmdb_msg),
        [1].iov_base = (void *)user_buf,
        [1].iov_len = user_buf_size,
    };

    struct iovec reply_iov = {
        .iov_base = (void *)&pcreq->pcreq_msg_reply,
        .iov_len = sizeof(struct pmdb_msg),
    };

    int opts = RCRT_WRITE;
    if (pmdb_req_opt->pro_non_blocking)
        opts |= RCRT_NON_BLOCKING;

    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui, req_iovs, 2,
                                      &reply_iov, 1, false,
                                      pmdb_req_opt->pro_timeout,
                                      opts,
                                      pmdb_client_request_cb, pcreq,
                                      pcreq->pcreq_tag);
}

/**
 * PmdbObjPut - blocking public put (write) routine.
 */
int
PmdbObjPut(pmdb_t pmdb, const pmdb_obj_id_t *obj_id, const char *kv,
           size_t kv_size, struct pmdb_obj_stat *user_pmdb_stat)
{
    pmdb_request_opts_t pmdb_req_opt;

    pmdb_request_options_init(&pmdb_req_opt, 1, 0, user_pmdb_stat, NULL, NULL,
                              NULL, 0, pmdb_get_default_request_timeout());

    return pmdb_obj_put_internal(pmdb, obj_id, kv, kv_size,
                                 &pmdb_req_opt);
}

/**
 * PmdbObjPutNB - public put (write) routine(Blocking or Non-blocking).
 */
int
PmdbObjPutX(pmdb_t pmdb, const pmdb_obj_id_t *obj_id, const char *kv,
             size_t kv_size, pmdb_request_opts_t *pmdb_req_opt)
{
    /* Non blocking call should have callback function pointer */
    if (!pmdb_req_opt || (pmdb_req_opt->pro_non_blocking &&
                          !pmdb_req_opt->pro_non_blocking_cb))
        return -EINVAL;

    /* Use default timeout if not set by user. */
    pmdb_check_and_set_default_timeout(pmdb_req_opt);

    return pmdb_obj_put_internal(pmdb, obj_id, kv, kv_size,
                                 pmdb_req_opt);
}

static int
pmdb_obj_get_internal(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                      const void *key, size_t key_size,
                      pmdb_request_opts_t *pmdb_req_opt)
{
    // NULL user_buf or buf_size of 0 is OK
    if (!pmdb || !obj_id || (pmdb_req_opt->pro_non_blocking &&
                             !pmdb_req_opt->pro_non_blocking_cb))
    {
        SIMPLE_LOG_MSG(LL_WARN, "Return from here1");
        return -EINVAL;
    }

    int rc = 0;

    struct pmdb_client_request *pcreq =
        pmdb_client_request_new(obj_id, pmdb_op_read, key, key_size,
                                pmdb_req_opt->pro_get_buffer,
                                pmdb_req_opt->pro_get_buffer_size,
                                pmdb_req_opt->pro_stat,
                                pmdb_req_opt->pro_timeout,
                                pmdb_req_opt->pro_non_blocking_cb,
                                pmdb_req_opt->pro_arg, &rc);
    if (!pcreq)
        return rc;

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    struct iovec req_iovs[2] = {
        [0].iov_base = (void *)&pcreq->pcreq_msg_request,
        [0].iov_len = sizeof(struct pmdb_msg),
        [1].iov_base = (void *)key,
        [1].iov_len = key_size,
    };

    struct iovec reply_iovs[2] = {
        [0].iov_base = (void *)&pcreq->pcreq_msg_reply,
        [0].iov_len = sizeof(struct pmdb_msg),
        [1].iov_base = pmdb_req_opt->pro_get_buffer,
        [1].iov_len = pmdb_req_opt->pro_get_buffer_size,
    };

    /* Set request type as READ and blocking or non-blocking operation */
    int opts = RCRT_READ;
    if (pmdb_req_opt->pro_non_blocking)
        opts |= RCRT_NON_BLOCKING;

    // Note: we should probably only send 1 iov in the case of !pro_get_buffer.
    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui, req_iovs, 2,
                                      reply_iovs, 2,
                                      (pmdb_req_opt->pro_get_buffer == NULL ?
                                       true : false),
                                      pmdb_req_opt->pro_timeout,
                                      opts,
                                      pmdb_client_request_cb, pcreq,
                                      pcreq->pcreq_tag);

}

/**
 * PmdbObjGetX - public get (read) routine (blocking or non-blocking).
 */
int
PmdbObjGetX(pmdb_t pmdb, const pmdb_obj_id_t *obj_id, const char *key,
            size_t key_size, pmdb_request_opts_t *pmdb_req_opt)
{
    /* Use default timeout if not set by user. */
    pmdb_check_and_set_default_timeout(pmdb_req_opt);
    return pmdb_obj_get_internal(pmdb, obj_id, key, key_size,
                                 pmdb_req_opt);
}

/**
 * PmdbObjGet - blocking public get (read) routine.
 */
void *
PmdbObjGet(pmdb_t pmdb, const pmdb_obj_id_t *obj_id, const char *key,
           size_t key_size, size_t *value_size)
{
    struct pmdb_obj_stat pmdb_stat;
    pmdb_request_opts_t pmdb_req_opt;

    /* Initialize the request options */
    pmdb_request_options_init(&pmdb_req_opt, 0, 0, &pmdb_stat, NULL, NULL,
                              NULL, 0, pmdb_get_default_request_timeout());

    int rc = pmdb_obj_get_internal(pmdb, obj_id, key, key_size, &pmdb_req_opt);

    /* Copy the actual reply size */
    *value_size = pmdb_stat.reply_size;

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "Read request failed: error: %d", rc);
        return NULL;
    }
    return pmdb_stat.reply_buffer;
}

/**
 * pmdb_obj_id_cb - essential cb function which is passed into
 *    raft_client_init().  The role of this cb is to translate the private
 *    id structure into a raft_net_client_user_id for use by the raft client
 *    layer.
 */
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

/**
 * PmdbClientStart - public initialization routine.
 */
pmdb_t
PmdbClientStart(const char *raft_uuid_str, const char *raft_client_uuid_str)
{
    if (!raft_uuid_str || !raft_client_uuid_str)
    {
        errno = -EINVAL;
        return NULL;
    }

    pmdb_t pmdb = NULL;

	SIMPLE_LOG_MSG(LL_WARN, "Inside PmdbClientStart");
    int rc = raft_client_init(raft_uuid_str, raft_client_uuid_str,
                              pmdb_obj_id_cb, &pmdb,
                              RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP);
    if (rc)
    {
        errno = -rc;
        return NULL;
    }

    NIOVA_ASSERT(pmdb);

    return pmdb;
}

char *
PmdbGetLeaderUUID(pmdb_t pmdb)
{
    return raft_client_get_leader_uuid(pmdb_2_rci(pmdb));
}
