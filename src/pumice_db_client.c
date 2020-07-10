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

    return pmdb;
}
