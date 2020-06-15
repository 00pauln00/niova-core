/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_PUMICE_DB_H_
#define __NIOVA_PUMICE_H_ 1

#include <uuid/uuid.h>
#include <rocksdb/c.h>

#include "common.h"

enum PmdbOpType
{
    pmdb_op_noop = 0,
    pmdb_op_read = 1,
    pmdb_op_write = 2,
    pmdb_op_apply = 3,
    pmdb_op_none = 4,
};

typedef void
(*pmdb_apply_sm_handler_t)(uuid_t, const char *, size_t,
                           rocksdb_writebatch_t *);

/**
 * pmdb_read_sm_handler_t - performs a general read operation. The app-uuid and
 *    the requisite buffers are provided.  The implementation must provide the
 *    the number of bytes used in reply_buf.
 */
typedef ssize_t
(*pmdb_read_sm_handler_t)(uuid_t app_uuid, const char *request_buf,
                          size_t request_bufsz, char *reply_buf,
                          size_t reply_bufsz);

/**
 * pmdb_interpret_sm_handler_t - responsible for parsing the request contents
 *   to determine if the operation should proceed, and if so, the type of the
 *   operation - read or write.  A return of pmdb_op_noop will cause a client
 *   return with no error whereas a return pmdb_op_none will cause -EINVAL to
 *   be returned.
 */
typedef enum PmdbOpType
(*pmdb_interpret_sm_handler_t)(uuid_t app_uuid, const char *request_buf,
                               size_t request_bufsz);

struct PmdbAPI
{
    pmdb_apply_sm_handler_t     pmdb_apply;
    pmdb_read_sm_handler_t      pmdb_read;
    pmdb_interpret_sm_handler_t pmdb_interpret;
};

int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api);

int
PmdbClose(void);

#endif
