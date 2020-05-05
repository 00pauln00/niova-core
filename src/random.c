/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include <syscall.h>
#include <uuid/uuid.h>

#include "common.h"
#include "log.h"
#include "random.h"
#include "util.h"

REGISTRY_ENTRY_FILE_GENERATE;

static __thread struct random_data randomData;
static __thread char randomStateBuf[RANDOM_STATE_BUF_LEN];
static __thread bool randInit;
static __thread unsigned int randSeed = 1040071U;

pid_t gettid(void)
{
    pid_t tid = syscall(SYS_gettid);

    return tid;
}

uint32_t
random_create_seed_from_uuid(const uuid_t uuid)
{
    // Generate the msg-id using our UUID as a base.
    uint64_t uuid_int[2];
    niova_uuid_2_uint64(uuid, &uuid_int[0], &uuid_int[1]);

    const uint32_t *ptr = (const uint32_t *)&uuid_int;

    uint32_t seed = ptr[0] ^ ptr[1] ^ ptr[2] ^ ptr[3];

    return seed;
}

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
        random_init(gettid() ^ randSeed);

    unsigned int result;
    if (random_r(&randomData, (int *)&result))
        log_msg(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
}
