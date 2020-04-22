/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef _SYSTEM_INFO_H
#define _SYSTEM_INFO_H 1

#include "init.h"
#include "ctor.h"
#include "env.h"

int
system_info_apply_uuid_by_str(const char *uuid_str);

env_cb_ctx_t
system_info_apply_uuid_env_cb(const struct niova_env_var *ev);

init_ctx_t
system_info_subsystem_init(void)
    __attribute__ ((constructor (SYSTEM_INFO_CTOR_PRIORITY)));

destroy_ctx_t
system_info_subsystem_destroy(void)
    __attribute__ ((destructor (SYSTEM_INFO_CTOR_PRIORITY)));

#endif
