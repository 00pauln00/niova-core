/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include "niova_backtrace.h"

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

static void
ws_test_cb(void *arg)
{
    NIOVA_ASSERT(arg);

    int *handle = (int *)arg;
    *handle += 1;
}

static void
ws_test(void)
{
    struct raft_net_sm_write_supplements ws = {0};

    // noop
    raft_net_sm_write_supplement_destroy(&ws);

    raft_net_sm_write_supplement_init(&ws);

    int rc = raft_net_sm_write_supplements_merge(&ws, NULL);
    NIOVA_ASSERT(rc == -EINVAL);

    int handle = 0;

    rc = raft_net_sm_write_supplement_add(&ws, (void *)&handle, ws_test_cb,
                                          "foo", 3, "bar", 3);
    NIOVA_ASSERT(rc == 0);

    NIOVA_ASSERT(ws.rnsws_nitems == 1);

    raft_net_sm_write_supplement_destroy(&ws);
    NIOVA_ASSERT(handle == 1);

    handle = 0;

    for (int i = 0; i < RAFT_NET_WR_SUPP_MAX; i++)
    {
        rc = raft_net_sm_write_supplement_add(&ws, (void *)&handle, ws_test_cb,
                                              "foo", 3, "bar", 3);
        NIOVA_ASSERT(!rc);
    }

    FATAL_IF((ws.rnsws_nitems != RAFT_NET_WR_SUPP_MAX), "nitems == %zu",
             ws.rnsws_nitems);
    NIOVA_ASSERT(handle == 0);

    rc = raft_net_sm_write_supplement_add(&ws, (void *)&handle, ws_test_cb,
                                          "foo", 3, "bar", 3);
    NIOVA_ASSERT(rc == -ENOSPC);

    // Try a merge and assert that it does not fit
    struct raft_net_sm_write_supplements wsMerge = {0};
    rc = raft_net_sm_write_supplement_add(&wsMerge, (void *)&handle, ws_test_cb,
                                          "foo", 3, "bar", 3);
    NIOVA_ASSERT(!rc);
    rc = raft_net_sm_write_supplements_merge(&ws, &wsMerge);
    NIOVA_ASSERT(rc == -E2BIG);

    // Destroy 'ws', add an item, and retry the merge
    raft_net_sm_write_supplement_destroy(&ws);
    NIOVA_ASSERT(handle == RAFT_NET_WR_SUPP_MAX);
    handle = 0;

    rc = raft_net_sm_write_supplement_add(&ws, (void *)&handle, ws_test_cb,
                                          "foo", 3, "bar", 3);
    NIOVA_ASSERT(!rc);

    rc = raft_net_sm_write_supplements_merge(&ws, &wsMerge);
    NIOVA_ASSERT(!rc);
    NIOVA_ASSERT(ws.rnsws_nitems == 2);
    NIOVA_ASSERT(wsMerge.rnsws_nitems == 0 && wsMerge.rnsws_ws == NULL);

    // Ensure that both destroy cb's are run
    raft_net_sm_write_supplement_destroy(&ws);
    NIOVA_ASSERT(handle == 2);
}

int
main(void)
{
    struct raft_net_client_user_id rncui = {0};

    vote_sort();
    ws_test();

    int rc = raft_net_client_user_id_parse(
        "1a636bd0-d27d-11ea-8cad-90324b2d1e89:2341523123:32452300123:1:0",
        &rncui, 0);

    NIOVA_ASSERT(!rc);

    return rc;
}
