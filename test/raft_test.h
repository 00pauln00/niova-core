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
    ((RAFT_NET_MAX_RPC_SIZE -                                           \
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

/**
 * -- struct raft_test_data_block --
 * Raft test client data block - this is the payload which is stored in the
 *    raft log as "application data".
 * @rtdb_client_uuid: the unique identifier of the issuing "client".
 * @rtdb_num_values: number of test values found in this block.
 * @rtdb__pad: future use?
 * @rtdb_values: payload data
 */
struct raft_test_data_block
{
    uuid_t                  rtdb_client_uuid; // application uuid
    uint16_t                rtdb_num_values;
    uint16_t                rtdb__pad[3];
    struct raft_test_values rtdb_values[];
};

static inline void
raft_test_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(RAFT_TEST_VALUES_MAX > 0);
}

#endif
