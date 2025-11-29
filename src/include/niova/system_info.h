/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef _SYSTEM_INFO_H
#define _SYSTEM_INFO_H 1

#include <uuid/uuid.h>

#include "niova/init.h"
#include "niova/ctor.h"
#include "niova/env.h"

void
    system_info_get_uuid(uuid_t);

bool
system_info_uuid_is_present(void);

int
system_info_apply_uuid_by_str(const char *uuid_str);

env_cb_ctx_t
system_info_apply_uuid_env_cb(const struct niova_env_var *ev);

#endif
