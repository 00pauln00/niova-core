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

#include "common.h"
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

#define niova_calloc_can_fail(nmemb, size)                                \
({                                                                        \
    void *ptr = calloc(nmemb, size);                                      \
    LOG_MSG(allocLogLevel, "niova_calloc: %p %zu %zu", ptr, nmemb, size); \
    ptr;                                                                  \
})

#define niova_posix_memalign(size, alignment)                      \
({                                                                 \
    void *ptr = NULL;                                              \
    int rc = posix_memalign(&ptr, alignment, size);                \
    if (rc) ptr = NULL;                                            \
    LOG_MSG((rc ? LL_ERROR : allocLogLevel),                       \
            "niova_posix_memalign: %p %zu %zu: %s",                \
            ptr, size, alignment, rc ? strerror(rc) : "OK");       \
    ptr;                                                           \
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

#define NIOVA_VBA_MAX_BITS (NBBY * sizeof(uint64_t))

struct niova_vbasic_allocator
{
    const size_t nvba_unit_size; // size represented by each bit
    uint64_t     nvba_bitmap;
    char         nvba_region[];
};

static inline size_t
niova_vbasic_nassigned(const struct niova_vbasic_allocator *nvba)
{
    if (!nvba || nvba->nvba_bitmap == -1ULL)
        return NIOVA_VBA_MAX_BITS;

    else if (nvba->nvba_bitmap == 0)
        return 0;

    return number_of_ones_in_val(nvba->nvba_bitmap);
}

static inline int
niova_vbasic_init(struct niova_vbasic_allocator *nvba,
                  size_t region_size)
{
    if (!nvba)
        return -EINVAL;

    unsigned int unit_size = region_size / NIOVA_VBA_MAX_BITS;
    if (unit_size == 0)
        return -EINVAL;

    CONST_OVERRIDE(size_t, nvba->nvba_unit_size, unit_size);
    nvba->nvba_bitmap = 0;

    return 0;
}

static inline int
niova_vbasic_space_avail(const struct niova_vbasic_allocator *nvba,
                         size_t size_in_bytes)
{
    if (!nvba || !size_in_bytes)
        return -EINVAL;

    const unsigned int nunits =
        (size_in_bytes / nvba->nvba_unit_size +
         (size_in_bytes % nvba->nvba_unit_size ? 1 : 0));

    if (nunits > NIOVA_VBA_MAX_BITS)
        return -E2BIG;

    int rc = nconsective_bits_avail(&nvba->nvba_bitmap, nunits);

    return rc >= 0 ? 0 : rc; // return '0' on successful_ping_until_viable
}

static inline int
niova_vbasic_malloc(struct niova_vbasic_allocator *nvba, size_t size_in_bytes,
                    void **ret_ptr)
{
    if (!nvba || !size_in_bytes || !ret_ptr)
        return -EINVAL;

    const unsigned int nunits =
        (size_in_bytes / nvba->nvba_unit_size +
         (size_in_bytes % nvba->nvba_unit_size ? 1 : 0));

    if (nunits > NIOVA_VBA_MAX_BITS)
        return -E2BIG;

    int offset = nconsective_bits_assign(&nvba->nvba_bitmap, nunits);
    if (offset < 0)
        return offset;

    char *ptr = &nvba->nvba_region[offset * nvba->nvba_unit_size];

    *ret_ptr = ptr;

    return 0;
}

static inline int
niova_vbasic_free(struct niova_vbasic_allocator *nvba, const void *ptr,
                  size_t size_in_bytes)
{
    if (!nvba || !ptr || !size_in_bytes)
        return -EINVAL;

    const char *my_ptr = (const char *)ptr;

    if (my_ptr < &nvba->nvba_region[0] ||
        my_ptr > &nvba->nvba_region[NIOVA_VBA_MAX_BITS * nvba->nvba_unit_size])
        return -ERANGE; // ptr value not within the allocation region

    const uintptr_t ptr_diff = my_ptr - &nvba->nvba_region[0];

    if (ptr_diff % nvba->nvba_unit_size)
        return -EFAULT; // ptr is not aligned with the unit size

    unsigned int offset = ptr_diff / nvba->nvba_unit_size;

    const unsigned int nunits =
        (size_in_bytes / nvba->nvba_unit_size +
         (size_in_bytes % nvba->nvba_unit_size ? 1 : 0));

    return nconsective_bits_release(&nvba->nvba_bitmap, offset, nunits);
}

#endif
