/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "random.h"
#include "common.h"
#include "log.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define RANDOM_STATE_BUF_LEN 32
static __thread struct random_data randomData;
static __thread char randomStateBuf[RANDOM_STATE_BUF_LEN];
static __thread bool randInit;
static __thread unsigned int randSeed = 1040071U;

int
random_init(unsigned int seed)
{
    if (!randInit)
    {
        randInit = true;

        if (initstate_r(seed, randomStateBuf, RANDOM_STATE_BUF_LEN,
                        &randomData))
            log_msg(LL_FATAL, "initstate_r() failed: %s", strerror(errno));

        return 0;
    }

    return -EALREADY;
}

unsigned int
random_get(void)
{
    if (!randInit)
        random_init(randSeed);


    unsigned int result;
    if (random_r(&randomData, (int *)&result))
        log_msg(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
}
