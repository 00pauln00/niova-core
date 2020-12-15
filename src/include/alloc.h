/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */
#ifndef NIOVA_ALLOC_H
#define NIOVA_ALLOC_H 1

#ifndef __USE_GNU // reallocarray
#define __USE_GNU
#endif
#include <stdlib.h>

#include "log.h"

extern enum log_level allocLogLevel;

#define niova_malloc(size)                                     \
({                                                             \
    void *ptr = malloc(size);                                  \
    FATAL_IF_strerror((!ptr), "niova_malloc: ");               \
    LOG_MSG(allocLogLevel, "niova_malloc: %p %zu", ptr, size); \
    ptr;                                                       \
})

#define niova_malloc_can_fail(size)                            \
({                                                             \
    void *ptr = malloc(size);                                  \
    LOG_MSG(allocLogLevel, "niova_malloc: %p %zu", ptr, size); \
    ptr;                                                       \
})

#define niova_calloc(nmemb, size)                                         \
({                                                                        \
    void *ptr = calloc(nmemb, size);                                      \
    FATAL_IF_strerror((!ptr), "niova_calloc: ");                          \
    LOG_MSG(allocLogLevel, "niova_calloc: %p %zu %zu", ptr, nmemb, size); \
    ptr;                                                                  \
})

#define niova_realloc(oldptr, size)                                       \
({                                                                        \
    void *ptr = realloc(oldptr, size);                                    \
    FATAL_IF_strerror((!ptr), "niova_realloc: ");                         \
    LOG_MSG(allocLogLevel, "niova_realloc: %p  %zu", ptr, size);          \
    ptr;                                                                  \
})

#define niova_calloc_can_fail(nmemb, size)                                \
({                                                                        \
    void *ptr = calloc(nmemb, size);                                      \
    LOG_MSG(allocLogLevel, "niova_calloc: %p %zu %zu", ptr, nmemb, size); \
    ptr;                                                                  \
})

#define niova_free(ptr)                            \
{                                                  \
    free(ptr);                                     \
    LOG_MSG(allocLogLevel, "niova_free: %p", ptr); \
}

#define niova_reallocarray(ptr, type, nmemb)                           \
({                                                                     \
    type *tmp = reallocarray((ptr), nmemb, sizeof(type));              \
                                                                       \
    LOG_MSG(allocLogLevel, "niova_reallocarray: src=%p dst=%p sz=%zu", \
            ptr, tmp, (size_t)(sizeof(type) * nmemb));                 \
                                                                       \
    if (tmp)                                                           \
        (ptr) = tmp;                                                   \
                                                                       \
    tmp ? 0 : -ENOMEM;                                                 \
})

void
alloc_log_level_set(enum log_level ll);

struct niova_env_var;

void
alloc_env_var_cb(const struct niova_env_var *nev);

#endif
