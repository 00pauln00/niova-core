/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include "env.h"
#include "ctor.h"
#include "log.h"

REGISTRY_ENTRY_FILE_GENERATE;

enum log_level allocLogLevel = LL_DEBUG;

void
alloc_log_level_set(enum log_level ll)
{
    allocLogLevel = ll;
}

init_ctx_t
alloc_subsys_init(void)
{
    FUNC_ENTRY(LL_DEBUG);

    const struct niova_env_var *ev = env_get(NIOVA_ENV_VAR_alloc_log_level);

    if (ev && ev->nev_present)
	log_level_set(ev->nev_long_value);
};

destroy_ctx_t
alloc_subsys_destroy(void)
{
    FUNC_ENTRY(LL_DEBUG);
};
