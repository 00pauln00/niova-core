/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include "raft_net.h"

int main(void)
{
    struct raft_net_client_user_id rncui = {0};

    int rc = raft_net_client_user_id_parse(
        "1a636bd0-d27d-11ea-8cad-90324b2d1e89:2341523123:32452300123:1:0",
        &rncui, 0);

    return rc;
}
