/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */

#include "alloc.h"
#include "random.h"

REGISTRY_ENTRY_FILE_GENERATE;

static void
niova_vbasic_alloc_test2(void)
{
    struct niova_vbasic_allocator *nvba =
        niova_malloc(sizeof(struct niova_vbasic_allocator) + 64);

    int rc = niova_vbasic_init(nvba, 64); // 1 byte allocations
    NIOVA_ASSERT(!rc);

    for (int x = 0; x < 10000; x++)
    {
        int nallocs = 0;
        struct
        {
            void *ptr;
            size_t size;
        } allocstuff[257] = {0};

        for (int i = 0; i < 257; i++)
        {
            void *ptr;

            unsigned int rand = random_get();
            size_t cnt = MAX(1, (rand % MAX(1, x) % 64));
            int rc = niova_vbasic_malloc(nvba, cnt, &ptr);

//            fprintf(stdout, "rc=%d i=%d cnt=%zu ptr=%p\n", rc, i, cnt, ptr);

            NIOVA_ASSERT(rc == 0 || rc == -ENOSPC);

            if (rc >= 0)
            {
                allocstuff[nallocs].ptr = ptr;
                allocstuff[nallocs++].size = cnt;

                *(char *)ptr = 'x';

                if (i > 3 && !(i % 2)) // release one the assigned items
                {
                    int idx = random_get() % nallocs;
                    if (allocstuff[idx].ptr != NULL)
                    {
                        NIOVA_ASSERT(!niova_vbasic_free(
                                         nvba, allocstuff[idx].ptr,
                                         allocstuff[idx].size));
                        allocstuff[idx].ptr = NULL;
                    }
                }
            }
            else if (nallocs) // allocation failed, try to release something
            {
                int idx = random_get() % nallocs;

                // Find something to release by looking forward
                while (allocstuff[idx].ptr != NULL && (idx < nallocs - 1))
                    idx++;

                if (allocstuff[idx].ptr != NULL)
                {
                    NIOVA_ASSERT(!niova_vbasic_free(
                                     nvba, allocstuff[idx].ptr,
                                     allocstuff[idx].size));
                    allocstuff[idx].ptr = NULL;
                }
            }
        } //end inner for loop

        // Cleanup by releasing any unreleased bit fields
        while (nallocs--)
        {
            if (allocstuff[nallocs].ptr != NULL)
            {
                NIOVA_ASSERT(!niova_vbasic_free(
                                 nvba, allocstuff[nallocs].ptr,
                                 allocstuff[nallocs].size));
                allocstuff[nallocs].ptr = NULL;
            }
        }
        NIOVA_ASSERT(nvba->nvba_bitmap == 0);
    }
    niova_free(nvba);
}

static void
niova_vbasic_alloc_test(void)
{
    struct niova_vbasic_allocator tnvba;

    NIOVA_ASSERT(niova_vbasic_init(NULL, 65536) == -EINVAL);
    NIOVA_ASSERT(niova_vbasic_init(&tnvba, 63) == -EINVAL); //below min size

    struct niova_vbasic_allocator *nvba =
        niova_malloc(sizeof(struct niova_vbasic_allocator) + 4001);

    // normalize (mask off minor bits)
    const size_t max_allocatable_size = (4001 / 64) * 64;

    NIOVA_ASSERT(nvba);

    int rc = niova_vbasic_init(nvba, 4001);
    NIOVA_ASSERT(!rc);

    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == 0);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, max_allocatable_size) == 0);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, max_allocatable_size + 1) == -E2BIG);
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == 0);

    char *ptr = NULL;
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, 0, (void *)&ptr) == -EINVAL);
    NIOVA_ASSERT(niova_vbasic_malloc(NULL, max_allocatable_size,
                                     (void *)&ptr) == -EINVAL);
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, max_allocatable_size, NULL) ==
                 -EINVAL);

    // Since 4001 is not evenly divisible by 64
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, max_allocatable_size + 1,
                                     (void *)&ptr) == -E2BIG);

    // alloc entire region
    NIOVA_ASSERT((niova_vbasic_malloc(nvba, max_allocatable_size,
                                      (void *)&ptr) == 0) &&
                 ptr == nvba->nvba_region_ptr);
    // enospc test
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, 1, (void *)&ptr) == -ENOSPC);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == -ENOSPC);
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == NIOVA_VBA_MAX_BITS);

    // free it
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, max_allocatable_size) == 0);
    // test for double free
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, max_allocatable_size) ==
                 -EBADSLT);

    niova_free(nvba);
}

static void
niova_vbasic_alloc_alignment_test(void)
{
    struct niova_vbasic_allocator tnvba;

    //Test1: add extra bytes to region
    //vba NULL
    NIOVA_ASSERT(niova_vbasic_init_aligned(NULL, 65536, 4096, 0) == -EINVAL);
    //alignment 0
    NIOVA_ASSERT(niova_vbasic_init_aligned(&tnvba, 65536, 4096, 0) == -EINVAL);

    //region start might be non-aligned, additional 15 bytes extra to carve
    //out aligned buffers (alignment = 16)
    struct niova_vbasic_allocator *nvba =
        niova_malloc(sizeof(struct niova_vbasic_allocator) + 4096 + (16 - 1));
    NIOVA_ASSERT(nvba);

    //region size zero
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 0, 64, 15) == -EINVAL);

    //unit size zero
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 0, 15) == -EINVAL);

    //alignment < unit size
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 64, 128) == -EINVAL);

    //alignment should be power of 2
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 64, 15) == -EINVAL);

    //unit size should be multiple of alignmnent
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 81, 16) == -EINVAL);

    //does not allow more than 64 num of units
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 32, 16) == -EINVAL);

    // going beyond 64 units
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096 + 1, 64, 16) == -EINVAL);

    //region smaller than 64
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 63, 64, 16) == -EINVAL);

    //ok
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 64, 16) == 0);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == 0);
    //1 bytes extra to check availability beyond region size
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 4096 + 1) == -E2BIG);
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == 0);

    void *ptr = NULL;
    // alloc entire region
    int rc = niova_vbasic_malloc(nvba, 4096, &ptr);
    NIOVA_ASSERT(rc == 0);

    //allocated buffer should be aligned, alignment 16
    NIOVA_ASSERT(((uintptr_t)ptr & (16 - 1)) == 0);
    //if base is not aligned then buffer address is aligned and
    //should match region ptr
    NIOVA_ASSERT((((uintptr_t)(&nvba->nvba_region[0]) & (16 - 1)) == 0) ||
                 (char *)ptr == nvba->nvba_region_ptr);

    // enospc test
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, 1, &ptr) == -ENOSPC);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == -ENOSPC);
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == NIOVA_VBA_MAX_BITS);

    // free it
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, 4096) == 0);
    // test for double free
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, 4096) == -EBADSLT);

    niova_free(nvba);
    nvba = NULL;

    //Test2: allocate region wthout extra bytes
    nvba = niova_malloc(sizeof(struct niova_vbasic_allocator) + 4096);
    NIOVA_ASSERT(nvba);

    rc = niova_vbasic_init_aligned(nvba, 4096, 64, 64);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == 0);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 4097) == -E2BIG);
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == 0);

    ptr = NULL;
    // alloc entire region
    rc = niova_vbasic_malloc(nvba, 4096, &ptr);
    NIOVA_ASSERT(rc == 0);

    //allocated buffer should be aligned, alignment 64
    NIOVA_ASSERT(((uintptr_t)ptr & (64 - 1)) == 0);
    //if base is not aligned then region size is less than expacted
    size_t fragmented =
        (size_t)((uintptr_t)nvba->nvba_region_ptr -
                 (uintptr_t)&nvba->nvba_region[0]);
    //fragmentaton should be less than alignment
    NIOVA_ASSERT(fragmented < 64);

    //ptr and region ptr should match
    NIOVA_ASSERT((char *)ptr == nvba->nvba_region_ptr);

    //free the vba
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, 4096) == 0);
    niova_free(nvba);
}

int
main(void)
{
    niova_vbasic_alloc_test();
    niova_vbasic_alloc_test2();
    niova_vbasic_alloc_alignment_test();

    return 0;
}
