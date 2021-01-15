/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_TEST_H_
#define __NIOVA_RAFT_TEST_H_ 1

#include "common.h"
#include "raft_net.h"

#define RAFT_TEST_VALUES_MAX                                            \
    ((raft_net_max_rpc_size(RAFT_INSTANCE_STORE_POSIX_FLAT_FILE) -      \
      (offsetof(struct raft_test_data_block, rtdb_values) +             \
       sizeof(struct raft_client_rpc_msg))) /                           \
     sizeof(struct raft_test_values))

/**
 * -- struct raft_test_values --
 * Payload data for raft_test_data_block.
 * @rtv_seqno: application monotonic sequence number associated with value.
 * @rtv_request_value:  used in request context to convey the next value in the
 *   "sequence".
 * @rtv_reply_xor_all_values:  reply context value which is the XOR of all
 *   preceding sequence values.  This summed value's role is to prove to the
 *   application that all sequence values have been stored in Raft and
 *   processed by the server-side "state machine".
 */
struct raft_test_values
{
    uint64_t rtv_seqno;
    union
    {
        uint64_t rtv_request_value;
        uint64_t rtv_reply_xor_all_values;
    };
};

enum raft_test_data_op
{
    RAFT_TEST_DATA_OP_NONE = 0,
    RAFT_TEST_DATA_OP_READ = 1,
    RAFT_TEST_DATA_OP_WRITE = 2,
};

static inline const char *
raft_test_data_op_2_string(enum raft_test_data_op op)
{
    switch (op)
    {
    case RAFT_TEST_DATA_OP_NONE:
        return "none";
    case RAFT_TEST_DATA_OP_READ:
        return "read";
    case RAFT_TEST_DATA_OP_WRITE:
        return "write";
    default:
        break;
    }
    return "unknown";
}

/**
 * -- struct raft_test_data_block --
 * Raft test client data block - this is the payload which is stored in the
 *    raft log as "application data".
 * @rtdb_client_uuid: the unique identifier of the issuing "client".
 * @rtdb_op:  read or write
 * @rtdb_num_values: number of test values in rpc (used for write only)
 * @rtdb__pad: future use?
 * @rtdb_values: payload data (used for write only)
 */
struct raft_test_data_block
{
    uuid_t                  rtdb_client_uuid; // application uuid
    uint16_t                rtdb_op;
    uint32_t                rtdb_num_values;
    struct raft_test_values rtdb_values[];
};

static inline void
raft_test_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(RAFT_TEST_VALUES_MAX > 0);
}

static inline ssize_t
raft_test_data_block_total_size(const struct raft_test_data_block *rtdb)
{
    if (!rtdb)
        return (ssize_t)-EINVAL;
    else if (rtdb->rtdb_num_values > RAFT_TEST_VALUES_MAX)
        return (ssize_t)-EOVERFLOW;

    return (sizeof(struct raft_test_data_block) +
            (rtdb->rtdb_num_values * sizeof(struct raft_test_values)));
}

#define DBG_RAFT_TEST_DATA_BLOCK(log_level, rtdb, fmt, ...)          \
{                                                                    \
    char __uuid_str[UUID_STR_LEN];                                   \
    uuid_unparse((rtdb)->rtdb_client_uuid, __uuid_str);              \
    LOG_MSG(log_level, "%s op=%s nv=%u seqno=%ld val=%ld "fmt,       \
            __uuid_str, raft_test_data_op_2_string((rtdb)->rtdb_op), \
            (rtdb)->rtdb_num_values,                                 \
            ((rtdb)->rtdb_num_values > 0 ?                           \
             (rtdb)->rtdb_values[0].rtv_seqno : -1),                 \
            ((rtdb)->rtdb_num_values > 0 ?                           \
             (rtdb)->rtdb_values[0].rtv_request_value : -1),         \
            ##__VA_ARGS__);                                          \
}

#endif
