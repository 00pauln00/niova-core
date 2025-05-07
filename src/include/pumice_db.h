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

typedef ssize_t pumicedb_apply_ctx_ssize_t;
typedef ssize_t pumicedb_write_prep_ctx_ssize_t;
typedef ssize_t pumicedb_read_ctx_ssize_t;
typedef void    pumicedb_init_ctx_void_t;

//common arguments for pumicedb callback functions.
struct pumicedb_cb_cargs
{
    const struct raft_net_client_user_id *pcb_userid;
    const void                           *pcb_req_buf;
    size_t                                pcb_req_bufsz;
    char                                 *pcb_reply_buf;
    size_t                                pcb_reply_bufsz;
    enum raft_init_state_type             pcb_init;
    int                                  *pcb_continue_wr;
    void                                 *pcb_pmdb_handler;
    void                                 *pcb_user_data;
};

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
typedef pumicedb_apply_ctx_ssize_t
(*pmdb_apply_sm_handler_t)(struct pumicedb_cb_cargs *args);

/**
 * pmdb_read_sm_handler_t - performs a general read operation. The app-uuid and
 *    the requisite buffers are provided.  The implementation must provide the
 *    the number of bytes used in reply_buf.
 */
typedef pumicedb_read_ctx_ssize_t
(*pmdb_read_sm_handler_t)(struct pumicedb_cb_cargs *args);

/**
 * pmdb_write_prep_sm_handler_t - The write prepare handler is called from
 * raft before applying the write entry.
 * Application is presented with original buffer content. And looking at the
 * buffer data, application can decide whether to go ahead with write operation.
 * Actual write to rocksDB would happen only through apply handler.
 */
typedef pumicedb_write_prep_ctx_ssize_t
(*pmdb_write_prep_sm_handler_t)(struct pumicedb_cb_cargs *args);

/**
 *pmdb_init_sm_handler_t - The init peer handler is called from
 * raft when peer boots up, becomes leader.
 * It can be used if Application wants to perform some initialization/cleanup
 * on bootup/becoming leader or shutdown.
 */
typedef pumicedb_init_ctx_void_t
(*pmdb_init_sm_handler_t)(struct pumicedb_cb_cargs *args);

struct PmdbAPI
{
    pmdb_write_prep_sm_handler_t      pmdb_write_prep;
    pmdb_apply_sm_handler_t           pmdb_apply;
    pmdb_read_sm_handler_t            pmdb_read;
    pmdb_init_sm_handler_t            pmdb_init;
};

/**
 * PmdbGetLeaderTimeStamp - Fill up the leader TS (timestamp) in the ts pointer.
 * @ts: Pointer to raft_leader_ts for storing the leader timstamp information.
 */
int
PmdbGetLeaderTimeStamp(struct raft_leader_ts *ts);

/**
 * PmdbIsLeader - Return true if the peer is leader.
 */
bool
PmdbIsLeader(void);

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

void pumice_server_rncui_id_parse(const char *in,
                              struct raft_net_client_user_id *rncui,
                              const version_t version);
#endif
