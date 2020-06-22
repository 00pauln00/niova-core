/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_PUMICE_DB_H_
#define __NIOVA_PUMICE_H_ 1

#include <uuid/uuid.h>
#include <rocksdb/c.h>

#include "raft_net.h"
#include "common.h"

#define PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP 1024
#define PMDB_MAX_APP_RPC_PAYLOAD_SIZE_UDP \
    RAFT_NET_MAX_RPC_SIZE - RAFT_NET_MAX_RPC_SIZE

#define PMDB_MAX_APP_RPC_PAYLOAD_SIZE PMDB_MAX_APP_RPC_PAYLOAD_SIZE_UDP

/**
 * pmdb_apply_sm_handler_t -
 */
typedef void
(*pmdb_apply_sm_handler_t)(const uuid_t app_uuid, const char *input_buf,
                           size_t input_bufsz, void *pmdb_handle);

/**
 * pmdb_read_sm_handler_t - performs a general read operation. The app-uuid and
 *    the requisite buffers are provided.  The implementation must provide the
 *    the number of bytes used in reply_buf.
 */
typedef ssize_t
(*pmdb_read_sm_handler_t)(const uuid_t app_uuid, const char *request_buf,
                          size_t request_bufsz, char *reply_buf,
                          size_t reply_bufsz);

struct PmdbAPI
{
    pmdb_apply_sm_handler_t pmdb_apply;
    pmdb_read_sm_handler_t  pmdb_read;
};

int
PmdbWriteKV(const uuid_t app_uuid, void *pmdb_handle, const char *key,
            size_t key_len, const char *value, size_t value_len,
            void (*comp_cb)(void *), void *app_handle);

int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api);

int
PmdbClose(void);

#endif
