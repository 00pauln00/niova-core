/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_NET_H_
#define __NIOVA_RAFT_NET_H_ 1

#include "common.h"

struct raft_instance;

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode);

int
raft_net_instance_shutdown(struct raft_instance *ri);

int
raft_net_server_instance_run(const char *raft_uuid_str,
                             const char *my_uuid_str);

#endif
