/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_PUMICE_DB_CLIENT_H_
#define __NIOVA_PUMICE_DB_CLIENT_H_ 1

#include "raft_client.h"

typedef raft_client_instance_t pmdb_t;

pmdb_t
PmdbClientStart(const char *raft_uuid_str, const char *raft_client_uuid_str);

#endif
