/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_TEST_H_
#define __NIOVA_RAFT_TEST_H_ 1

#include "common.h"

#define RAFT_TEST_VALUES_MAX (60000 / sizeof(struct raft_test_values))

struct raft_test_values
{
    uint64_t rti_seqno;
    uint64_t rti_seqno_value;
    uint64_t rti_xor_all_values;
};

struct raft_test_data_block
{
    uuid_t                  rtdb_client_uuid; // application uuid
    uint16_t                rtdb_num_values;
    struct raft_test_values rtdb_values[];
};

#endif
