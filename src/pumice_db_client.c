/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <errno.h>
#include <stdlib.h>
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
pmdb_client_request_lookup_cb(struct pmdb_client_request *pcreq,
                              ssize_t status)
{
    NIOVA_ASSERT(pcreq && pcreq->pcreq_op == pmdb_op_lookup &&
                 pcreq->pcreq_user_cb);

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

    pcreq->pcreq_lookup_cb(pcreq->pcreq_user_arg, ret_status);
}

static void
pmdb_client_request_cb(void *arg, ssize_t status)
{
    NIOVA_ASSERT(arg);

    struct pmdb_client_request *pcreq = (struct pmdb_client_request *)arg;

    switch (pcreq->pcreq_op)
    {
    case pmdb_op_lookup:
        pmdb_client_request_lookup_cb(pcreq, status);
        break;
    case pmdb_op_read:
    case pmdb_op_write:
        pmdb_client_request_rw_cb(pcreq, status);
        break;
    default:
        break;
    }

    niova_free(pcreq);
}


static void
pmdb_client_request_msg_init(struct pmdb_client_request *pcreq,
                             const pmdb_obj_id_t *obj_id,
                             enum PmdbOpType op, const char *req_buf,
                             const size_t req_buf_size, char *reply_buf,
                             const size_t reply_buf_size,
                             const struct timespec ts,
                             void (*user_cb)(void *, ssize_t),
                             void *user_arg)
{
    NIOVA_ASSERT(pcreq && obj_id);

    memset(pcreq, 0, sizeof(struct pmdb_client_request));

    pcreq->pcreq_msg_request.pmdbrm_magic = PMDB_MSG_MAGIC;
    raft_net_client_user_id_copy(&pcreq->pcreq_msg_request.pmdbrm_user_id,
                                 &rncui);
    pcreq->pcreq_msg_request.pmdbrm_write_seqno = -1;
    pcreq->pcreq_msg_request.pmdbrm_op = pmdb_op_lookup;
    pcreq->pcreq_msg_request.pmdbrm_write_pending = 0;
    pcreq->pcreq_msg_request.pmdbrm_data_size = 0;

    NIOVA_CRC_OBJ(&pcreq->pcreq_msg_request, struct pmdb_msg, pmdbrm_crc, 0);

    pcreq->pcreq_op = op;
    pcreq->pcreq_user_request = req_buf;
    pcreq->pcreq_user_request_size = req_buf_size;

    pcreq->pcreq_user_reply = reply_buf;
    pcreq->pcreq_user_reply_size = reply_buf_size;

    pcreq->pcreq_timeout = timeout;
    pcreq->pcreq_user_cb = user_cb;
    pcreq->pcreq_user_arg = user_arg;
}



//XXX the cb arg should be replaced with a
/* XX this is what the cb should look like:
   void (*cb)(const PmdbRet_t *, void *arg) and use a wrapper cb for
       the cb-arg to raft_client_request_submit()
 */
static int
pmdb_obj_lookup_internal(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                         char *reply, const size_t reply_size, bool blocking,
                         void(*cb)(const struct raft_net_client_user_id *,
                                   void *, char *, size_t, int),
                         void *arg, const struct timespec timeout)
{
    if (!pmdb || !obj_id || !reply || !reply_size || (blocking && !cb))
        return -EINVAL;

    struct pmdb_client_request *pcreq =
        niova_malloc_can_fail(sizeof(struct pmdb_client_request));

    if (!pcreq)
        return -ENOMEM;

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);



// The reply args should be removed and handled in the cb, furthermore
    // there should be 2 callbacks, one for this file and one for the caller
    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui,
                                      (void *)&request, sizeof(request),
                                      (void *)reply, reply_size, timeout,
                                      blocking, cb, arg);
}

/*XXX What's the return data situation here?
  - we need the write_seqno, write pending, and the existence rc??
 */
int
PmdbObjLookup(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
              pmdb_ret_t *lookup_ret)
{
    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    struct pmdb_msg reply = {0};

    return pmdb_obj_lookup_internal(pmdb, obj_id, &reply,
                                    sizeof(struct pmdb_msg), true, NULL, NULL,
                                    timeout);
}

int
PmdbObjLookupNB(pmdb_t pmdb, const pmdb_obj_id_t *obj_id,
                struct pmdb_obj_stat *ret_stat, void (*cb)(void *, ssize_t),
                void *arg)
{
    if (!cb)
        return -EINVAL;

    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    return pmdb_obj_lookup_internal(pmdb, obj_id, false, cb, arg, timeout);
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
