/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include "common.h"
#include "raft_net.h"
#include "raft.h"

static int
vote_sort(void)
{
    raft_entry_idx_t idx;

    int rc = raft_server_get_majority_entry_idx(NULL, 0, &idx);
    NIOVA_ASSERT(rc == -EINVAL);

    raft_entry_idx_t e2big[] = {-1, 0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11};
    rc = raft_server_get_majority_entry_idx(e2big, ARRAY_SIZE(e2big), &idx);
    NIOVA_ASSERT(rc == -E2BIG);

    raft_entry_idx_t negative[] = {-1, -1, -1, -1, -1};
    rc = raft_server_get_majority_entry_idx(negative, ARRAY_SIZE(negative),
                                            &idx);
    NIOVA_ASSERT(!rc && idx == -1);

    raft_entry_idx_t negative2[] = {-1, -1, -1, 0, 1};
    rc = raft_server_get_majority_entry_idx(negative2, ARRAY_SIZE(negative2),
                                            &idx);
    NIOVA_ASSERT(!rc && idx == -1);

    raft_entry_idx_t pos_even[] = {5, 4, 3, 2, 1, 0};
    rc = raft_server_get_majority_entry_idx(pos_even, ARRAY_SIZE(pos_even),
                                            &idx);
    NIOVA_ASSERT(!rc && idx == 2);

    raft_entry_idx_t mix_even[] = {127, 4294967297, -1, -1};
    rc = raft_server_get_majority_entry_idx(mix_even, ARRAY_SIZE(mix_even),
                                            &idx);
    NIOVA_ASSERT(!rc && idx == -1);

    raft_entry_idx_t mix2[] = {127, 4294967297, -1, -1, 128};
    rc = raft_server_get_majority_entry_idx(mix2, ARRAY_SIZE(mix2), &idx);
    NIOVA_ASSERT(!rc && idx == 127);

    for (int i = 0; i < ARRAY_SIZE(mix2); i++)
    {
        switch (i)
        {
        case 0:
            NIOVA_ASSERT(mix2[i] == -1);
            break;
        case 1:
            NIOVA_ASSERT(mix2[i] == -1);
            break;
        case 2:
            NIOVA_ASSERT(mix2[i] == 127);
            break;
        case 3:
            NIOVA_ASSERT(mix2[i] == 128);
            break;
        case 4:
            NIOVA_ASSERT(mix2[i] == 4294967297);
            break;
        default:
            NIOVA_ASSERT(1);
            break;
        }
    }

    raft_entry_idx_t large[] = {-1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    rc = raft_server_get_majority_entry_idx(large, ARRAY_SIZE(large), &idx);
    NIOVA_ASSERT(!rc && idx == 4);

    return 0;
}

int main(void)
{
    struct raft_net_client_user_id rncui = {0};

    vote_sort();

    int rc = raft_net_client_user_id_parse(
        "1a636bd0-d27d-11ea-8cad-90324b2d1e89:2341523123:32452300123:1:0",
        &rncui, 0);

    NIOVA_ASSERT(!rc);

    return rc;
}
