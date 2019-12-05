/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include "env.h"
#include "log.h"

enum log_level allocLogLevel = LL_DEBUG;

void
alloc_log_level_set(enum log_level ll)
{
    allocLogLevel = ll;
}

void
alloc_env_var_cb(const struct niova_env_var *nev)
{
    if (nev && nev->nev_present)
        alloc_log_level_set((enum log_level)nev->nev_long_value);
}
