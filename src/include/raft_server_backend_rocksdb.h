/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef RAFT_SERVER_BACKEND_ROCKSDB_H
#define RAFT_SERVER_BACKEND_ROCKSDB_H 1

#include <rocksdb/c.h>

#include "raft.h"

#define RAFT_ROCKSDB_MAX_CF 32
#define RAFT_ROCKSDB_MAX_CF_NAME_LEN 4096

struct raft_server_rocksdb_cf_table
{
    const char                     *rsrcfe_cf_names[RAFT_ROCKSDB_MAX_CF];
    rocksdb_column_family_handle_t *rsrcfe_cf_handles[RAFT_ROCKSDB_MAX_CF];
    size_t                          rsrcfe_num_cf;
};

struct rocksdb_t *
raft_server_get_rocksdb_instance(struct raft_instance *ri);

int
raft_server_rocksdb_add_cf_name(struct raft_server_rocksdb_cf_table *cft,
                                const char *cf_name, const size_t cf_name_len);

void
raft_server_rocksdb_release_cf_table(struct raft_server_rocksdb_cf_table *cft);

#endif
