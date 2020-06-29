/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdlib.h>
#include <uuid/uuid.h>

#include "alloc.h"
#include "crc32.h"
#include "log.h"
#include "pumice_db_net.h"
#include "registry.h"

int
pmdbClientExec(const char *raft_uuid_str, const char *raft_client_uuid_str)
{
    if (!raft_uuid_str || !raft_client_uuid_str)
        return -EINVAL;

    struct raft_instance *ri = raft_net_get_instance();
    if (!ri)
        return -EINVAL;

    ri->ri_raft_uuid_str = raft_uuid_str;
    ri->ri_this_peer_uuid_str = my_uuid_str;

    raft_net_instance_apply_callbacks(ri, pmdb_client_timerfd_cb,
                                      raft_client_udp_recv_handler,
                                      NULL);


}
