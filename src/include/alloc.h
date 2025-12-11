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
    LOG_MSG(allocLogLevel, "niova_free: %p", ptr); \
    free(ptr);                                     \
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
    unsigned int nvba_alignment;
    uint32_t     nvba_unit_size; // size represented by each bit
    uint64_t     nvba_bitmap;
    char        *nvba_region_ptr;
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
niova_vbasic_init(struct niova_vbasic_allocator *nvba, size_t region_size)
{
    if (!nvba)
        return -EINVAL;

    unsigned int unit_size = region_size / NIOVA_VBA_MAX_BITS;

    if (unit_size == 0 || unit_size > UINT32_MAX)
        return -EINVAL;

    CONST_OVERRIDE(uint32_t, nvba->nvba_unit_size, unit_size);
    nvba->nvba_bitmap = 0;
    nvba->nvba_alignment = 0;

    nvba->nvba_region_ptr = &nvba->nvba_region[0];
    return 0;
}

/**
 * To callers: Always allocate region size inclusive of alignment
 */
static inline int
niova_vbasic_init_aligned(struct niova_vbasic_allocator *nvba,
                          size_t region_size, size_t unit_size,
                          size_t alignment)
{
    if (!nvba)
        return -EINVAL;

    if (region_size == 0)
        return -ENODATA;

    /* alignment is power of 2 */
    if (alignment == 0 || (alignment & (alignment - 1)))
        return -EDOM;

    unit_size = (unit_size == 0) ? region_size / NIOVA_VBA_MAX_BITS : unit_size;

    if (unit_size == 0 || unit_size > UINT32_MAX)
        return -EINVAL;

    if (alignment > unit_size)
        return -EOVERFLOW;

    /* Unit size is multiple of alignment */
    if (unit_size & (alignment - 1))
        return -EDOM;

    if (region_size < unit_size)
        return -EINVAL;

    const unsigned int nunits = region_size / unit_size;

    if (nunits > NIOVA_VBA_MAX_BITS)
        return -EOVERFLOW;

    CONST_OVERRIDE(uint32_t, nvba->nvba_unit_size, unit_size);
    nvba->nvba_bitmap = 0;
    nvba->nvba_alignment = alignment;

    /* Support < NIOVA_VBA_MAX_BITS num of units. Make rest of the bits in the
     * bitmap 1 so that those bits are unavailable.
     */
    if (nunits < NIOVA_VBA_MAX_BITS)
    {
        uint64_t bitmap_mask = (nunits == 0) ? 0ULL : ((1ULL << nunits) - 1ULL);
        nvba->nvba_bitmap = ~bitmap_mask;
    }

    uintptr_t raw = (uintptr_t)&nvba->nvba_region[0];
    uintptr_t aligned = alignment ? ALIGN_UP(raw, alignment) : raw;
    NIOVA_ASSERT(!alignment || (aligned & (alignment - 1)) == 0);

    nvba->nvba_region_ptr = (char *)aligned;
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

    char *ptr = &nvba->nvba_region_ptr[offset * nvba->nvba_unit_size];
    NIOVA_ASSERT(!nvba->nvba_alignment ||
                 ((uintptr_t)ptr & (nvba->nvba_alignment - 1)) == 0);

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

    if (my_ptr < nvba->nvba_region_ptr ||
        my_ptr > &nvba->nvba_region_ptr[NIOVA_VBA_MAX_BITS * nvba->nvba_unit_size])
        return -ERANGE; // ptr value not within the allocation region

    const uintptr_t ptr_diff = my_ptr - nvba->nvba_region_ptr;

    if (ptr_diff % nvba->nvba_unit_size)
        return -EFAULT; // ptr is not aligned with the unit size

    unsigned int offset = ptr_diff / nvba->nvba_unit_size;

    const unsigned int nunits =
        (size_in_bytes / nvba->nvba_unit_size +
         (size_in_bytes % nvba->nvba_unit_size ? 1 : 0));

    return nconsective_bits_release(&nvba->nvba_bitmap, offset, nunits);
}

static inline uintptr_t
niova_vbasic_get_start_addr(struct niova_vbasic_allocator *nvba)
{
    if (!nvba)
        return (uintptr_t) NULL;
    return (uintptr_t)nvba->nvba_region_ptr;
}
#endif
