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
                 ptr == &nvba->nvba_region[0]);
    // enospc test
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, 1, (void *)&ptr) == -ENOSPC);
    // free it
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, max_allocatable_size) == 0);
    // test for double free
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, max_allocatable_size) ==
                 -EBADSLT);

    niova_free(nvba);
}

int
main(void)
{
    niova_vbasic_alloc_test();
    niova_vbasic_alloc_test2();

    return 0;
}
