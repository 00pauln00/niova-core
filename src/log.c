/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "log.h"
#include <pthread.h>
#include <stdlib.h>

enum log_level dbgLevel = LL_WARN;

thread_id_t
thread_id_get(void)
{
    return pthread_self();
}

void
thread_abort(void)
{
    abort();
}
