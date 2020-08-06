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
static unsigned long pmdbClientDefaultTimeoutSecs = 120;

int
PmdbObjLookup(pmdb_t pmdb, const pmdb_obj_id_t *obj_id)
{
    if (!pmdb || !obj_id)
        return -EINVAL;

    struct raft_net_client_user_id rncui;
    NIOVA_ASSERT(pmdb_obj_id_2_rncui(obj_id, &rncui) == &rncui);

    PmdbRpcMsg_t request = {
        .pmdbrm_user_id = rncui,
        .pmdbrm_write_seqno = -1,
        .pmdbrm_op = pmdb_op_lookup,
        .pmdbrm_write_pending = 0,
        .pmdbrm_data_size = 0
    };

    PmdbRpcMsg_t reply = {0};

    const struct timespec timeout = {pmdbClientDefaultTimeoutSecs, 0};

    return raft_client_request_submit(pmdb_2_rci(pmdb), &rncui,
                                      (void *)&request, sizeof(request),
                                      (void *)&reply, sizeof(reply), timeout,
                                      true, NULL, NULL);
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

    int rc = raft_client_init(raft_uuid_str, raft_client_uuid_str, &pmdb);
    if (rc)
    {
        errno = -rc;
        return NULL;
    }

    NIOVA_ASSERT(pmdb);

    return pmdb;
}
