/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#include "common.h"
#include "log.h"
#include "util.h"

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

void
init_ctx_set(void)
{
    initCtx = true;
}

void
init_ctx_unset(void)
{
    initCtx = false;
}

void
destroy_ctx_set(void)
{
    destroyCtx = true;
}

init_ctx_t
init_start(void)
{
    NIOVA_ASSERT(!initCtx);
    NIOVA_ASSERT(!destroyCtx);

    initCtx = true;

    niova_set_tz("UTC", false);

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
}

init_ctx_t
init_complete(void)
{
    NIOVA_ASSERT(initCtx);
    NIOVA_ASSERT(!destroyCtx);

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");

    initCtx = false;
}

void
destroy_ctx_unset(void)
{
    destroyCtx = false;
}

destroy_ctx_t
destroy_start(void)
{
    NIOVA_ASSERT(!initCtx);
    NIOVA_ASSERT(!destroyCtx);

    destroyCtx = true;
    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye");
}

destroy_ctx_t
destroy_complete(void)
{
    NIOVA_ASSERT(!initCtx);
    NIOVA_ASSERT(destroyCtx);

    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye");
}
