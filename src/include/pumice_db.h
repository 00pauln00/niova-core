/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_PUMICE_DB_H_
#define __NIOVA_PUMICE_DB_H_ 1

#include <uuid/uuid.h>
#include <rocksdb/c.h>

#include "pumice_db_net.h"
#include "raft_net.h"
#include "raft.h"
#include "common.h"

typedef void    pumicedb_apply_ctx_t;
typedef int     pumicedb_apply_ctx_int_t;
typedef int     pumicedb_write_prep_ctx_int_t;
typedef ssize_t pumicedb_read_ctx_ssize_t;

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
typedef pumicedb_apply_ctx_int_t
(*pmdb_apply_sm_handler_t)(const struct raft_net_client_user_id *,
                           const void *input_buf, size_t input_bufsz,
                           char *reply_buf, size_t reply_bufsz,
                           void *pmdb_handle,
                           void *user_data);

/**
 * pmdb_read_sm_handler_t - performs a general read operation. The app-uuid and
 *    the requisite buffers are provided.  The implementation must provide the
 *    the number of bytes used in reply_buf.
 */
typedef pumicedb_read_ctx_ssize_t
(*pmdb_read_sm_handler_t)(const struct raft_net_client_user_id *,
                          const char *request_buf, size_t request_bufsz,
                          char *reply_buf, size_t reply_bufsz, void *user_data);

/**
 * pmdb_write_prep_sm_handler_t - The write prepare handler is called from
 * raft before applying the write entry.
 * Application is presented with original buffer content. And looking at the
 * buffer data, application can decide whether to go ahead with write operation.
 * Actual write to rocksDB would happen only through apply handler.
 */
typedef pumicedb_write_prep_ctx_int_t
(*pmdb_write_prep_sm_handler_t)(const struct raft_net_client_user_id *,
                                const void *input_buf, size_t input_bufsz,
                                char *reply_buf, size_t reply_bufsz,
                                void *user_data, int *continue_wr);

struct PmdbAPI
{
    pmdb_write_prep_sm_handler_t pmdb_write_prep;
    pmdb_apply_sm_handler_t      pmdb_apply;
    pmdb_read_sm_handler_t       pmdb_read;
};

/**
 * PmdbGetLeaderTimeStamp - Fill up the leader TS (timestamp) in the ts pointer.
 * @ts: Pointer to raft_leader_ts for storing the leader timstamp information.
 */
int
PmdbGetLeaderTimeStamp(struct raft_leader_ts *ts);

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
PmdbWriteKV(const struct raft_net_client_user_id *, void *pmdb_handle,
            const char *key, size_t key_len, const char *value,
            size_t value_len, void (*comp_cb)(void *), void *app_handle);

/**
 * PmdbExec - blocking API call used by a pumice-enabled application which
 *    starts the underlying raft process and waits for incoming requests.
 * @raft_uuid_str:  UUID of raft
 * @raft_instance_uuid_str:  UUID of this specific raft peer
 * @pmdb_api:  Function callbacks for read and apply.
 * @cf_names:  Array of RocksDB column family names to be used by application
 * @num_cf_names:  Size of cf_names[] array,
 * @use_synchronous_writes:  RocksDB uses synchronous writes
 * @use_coalesced_writes: Enabled coalesced writes.
 */
int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api, const char *cf_names[],
         int num_cf, bool use_synchronous_writes,
         bool use_coalesced_writes, void *user_data);

/**
 * PmdbClose - called from application context to shutdown the pumicedb exec
 *   thread.
 */
int
PmdbClose(void);

rocksdb_readoptions_t *
PmdbGetRoptionsWithSnapshot(const uint64_t seq_number, uint64_t *ret_seq);

void
PmdbPutRoptionsWithSnapshot(const uint64_t seq_number);

rocksdb_t *
PmdbGetRocksDB(void);

rocksdb_column_family_handle_t *
PmdbCfHandleLookup(const char *cf_name);

const char *
PmdbRncui2Key(const struct raft_net_client_user_id *rncui);

size_t
PmdbEntryKeyLen(void);

#endif
