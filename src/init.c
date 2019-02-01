/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "log.h"

static bool initCtx;
static bool destroyCtx;

/**
 * init_ctx - returns true when the process is starting up and executing its
 *    list of constructors, prior to entering main().
 */
bool
init_ctx(void)
{
    return initCtx;
}

/**
 * destroy_ctx - return true when the process is executing its destructors
 *    following the completion on main().
 */
bool
destroy_ctx(void)
{
    return destroyCtx;
}

init_ctx_t
init_start(void)
{
    NIOVA_ASSERT(!initCtx);
    NIOVA_ASSERT(!destroyCtx);

    initCtx = true;

    SIMPLE_LOG_MSG(LL_WARN, "hello");
    log_level_set_from_env();
}

init_ctx_t
init_complete(void)
{
    NIOVA_ASSERT(initCtx);
    NIOVA_ASSERT(!destroyCtx);

    SIMPLE_LOG_MSG(LL_WARN, "hello");

    initCtx = false;
}

destroy_ctx_t
destroy_start(void)
{
    NIOVA_ASSERT(!initCtx);
    NIOVA_ASSERT(!destroyCtx);

    destroyCtx = true;
    SIMPLE_LOG_MSG(LL_WARN, "goodbye");
}

destroy_ctx_t
destroy_complete(void)
{
    NIOVA_ASSERT(!initCtx);
    NIOVA_ASSERT(destroyCtx);

    SIMPLE_LOG_MSG(LL_WARN, "goodbye");
}
