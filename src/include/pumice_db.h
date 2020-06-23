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
 * pmdb_apply_sm_handler_t - The apply handler is called from raft after the
 *    local raft instance has learned that a previously unapplied raft entry
 *    has been committed.  From the application's perspective, this is the
 *    post-commit callback.  The application is presented with its original
 *    buffer contents and a handle which is used as the homonymous argument
 *    to PmdbWriteKV().  PmdbWriteKV() is used inside this handler to stage
 *    KV updates into a rocksDB writebatch which is maintained by pumiceDB.
 *    The updates staged via PmdbWriteKV() are written atomically into rocksDB
 *    along with other pumiceDB and raft internal metadata.
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

/**
 * PmdbWriteKV - to be called by the pumice-enabled application in 'apply'
 *    context only.  This call is used by the application to stage KVs for
 *    writing into rocksDB.  KVs added within a single instance of the 'apply'
 *    callback are atomically written to rocksDB.
 * @app_uuid:  UUID of the application instance
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
PmdbWriteKV(const uuid_t app_uuid, void *pmdb_handle, const char *key,
            size_t key_len, const char *value, size_t value_len,
            void (*comp_cb)(void *), void *app_handle);

/**
 * PmdbExec - blocking API call used by a pumice-enabled application which
 *    starts the underlying raft process and waits for incoming requests.
 * @raft_uuid_str:  UUID of raft
 * @raft_instance_uuid_str:  UUID of this specific raft peer
 * @pmdb_api:  Function callbacks for read and apply.
 */
int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api);

/**
 * PmdbClose - called from application context to shutdown the pumicedb exec
 *   thread.
 */
int
PmdbClose(void);

#endif
