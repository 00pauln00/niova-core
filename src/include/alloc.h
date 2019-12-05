/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */
#ifndef NIOVA_ALLOC_H
#define NIOVA_ALLOC_H 1

#include <stdlib.h>

#include "log.h"

extern enum log_level allocLogLevel;

#define niova_malloc(size)                                           \
({                                                                   \
    void *ptr = malloc(size);                                        \
    LOG_MSG(allocLogLevel, "niova_malloc: %p %zu", ptr, size);       \
    ptr;                                                             \
})

#define niova_calloc(nmemb, size)                                         \
({                                                                        \
    void *ptr = calloc(nmemb, size);                                      \
    LOG_MSG(allocLogLevel, "niova_calloc: %p %zu %zu", ptr, nmemb, size); \
    ptr;                                                                  \
})

#define niova_free(ptr)                                 \
{                                                       \
    free(ptr);                                          \
    LOG_MSG(allocLogLevel, "niova_free: %p", ptr);      \
}

void
alloc_log_level_set(enum log_level ll);

init_ctx_t
alloc_subsys_init(void)
    __attribute__ ((constructor (ALLOC_SUBSYS_CTOR_PRIORITY)));

destroy_ctx_t
alloc_subsys_destroy(void)
    __attribute__ ((destructor (ALLOC_SUBSYS_CTOR_PRIORITY)));

#endif
