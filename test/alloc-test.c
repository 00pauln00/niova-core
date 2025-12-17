/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */

#include "alloc.h"
#include "random.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define NUNITS_BITS 7
#define REGSZ_BITS 57
#define REGSZ_MASK ((1ULL << REGSZ_BITS) - 1)
#define NUNITS_MASK ((1ULL << NUNITS_BITS) - 1)

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
test_vbasic_random_stress_full(void)
{
    const size_t MAX_REGION_SIZE = (1ULL << 30); /* 1 GiB */

    for (int iter = 0; iter < 20000; iter++)
    {
        unsigned r = random_get();

        /* ---------------- Geometry ---------------- */

        size_t nunits = 1 + (r % 64);
        size_t shift = 4 + (r % 20);     /* 16B .. 8MB */
        size_t unit_size = 1ULL << shift;

        /* alignment: power-of-2 <= unit_size */
        size_t align_shift = random_get() % (shift + 1);
        size_t alignment = 1ULL << align_shift;

        size_t max_unit_size =
            MIN(((1ULL << 57) - 1) / nunits,
                MAX_REGION_SIZE / nunits);

        if (unit_size > max_unit_size)
            unit_size = max_unit_size;

        if (unit_size == 0)
            continue;

        size_t region_size = nunits * unit_size;
        if (region_size < unit_size)
            continue;

        /* ---------------- Allocation ---------------- */

        size_t total_size =
            sizeof(struct niova_vbasic_allocator) + region_size;

        void *raw = NULL;
        raw = niova_malloc(total_size);
        NIOVA_ASSERT(raw != NULL);

        struct niova_vbasic_allocator *nvba =
            (struct niova_vbasic_allocator *)raw;

        int rc = niova_vbasic_init_aligned(nvba, region_size, unit_size,
                                           alignment);

        /* ---------------- Init expectations ---------------- */

        if (rc != 0)
        {
            /*
             * All of these are VALID failures depending on geometry
             * and base alignment.
             */
            NIOVA_ASSERT(rc == -EINVAL   ||
                         rc == -EDOM     ||
                         rc == -ENODATA  ||
                         rc == -EOVERFLOW);
            niova_free(raw);
            continue;
        }

        /* ---------------- Alloc / Free churn ---------------- */

        uint64_t initial_bitmap = nvba->nvba_bitmap;
        struct
        {
            void   *ptr;
            size_t  units;
            uint8_t poison;
        } live[64] = {0};

        int nlive = 0;

        for (int step = 0; step < 1500; step++)
        {
            unsigned rv = random_get();
            if ((rv & 1) && nlive < (int)nunits)
            {
                size_t req_units = 1 + (rv % nunits);
                void *ptr = NULL;
                rc = niova_vbasic_malloc(nvba,
                                         req_units * unit_size,
                                         &ptr);
                NIOVA_ASSERT(rc == 0 || rc == -ENOSPC);
                if (rc == 0)
                {
                    NIOVA_ASSERT(IS_ALIGNED_PTR(ptr, alignment));

                    live[nlive].ptr   = ptr;
                    live[nlive].units = req_units;
                    live[nlive].poison =
                        (uint8_t)(0xA5 + (nlive & 0x1F));

                    uint8_t *p = ptr;
                    for (size_t u = 0; u < req_units; u++)
                        p[u * unit_size] = live[nlive].poison;

                    nlive++;
                }
            }
            else if (nlive > 0)
            {
                int idx = rv % nlive;
                uint8_t *p = live[idx].ptr;

                for (size_t u = 0; u < live[idx].units; u++)
                    NIOVA_ASSERT(p[u * unit_size] == live[idx].poison);

                NIOVA_ASSERT(niova_vbasic_free(nvba,
                             live[idx].ptr,live[idx].units * unit_size) == 0);
                live[idx] = live[--nlive];
            }
        }

        /* ---------------- Cleanup ---------------- */

        while (nlive--)
        {
            niova_vbasic_free(nvba,
                              live[nlive].ptr,
                              live[nlive].units * unit_size);
        }

        if (nvba->nvba_bitmap != initial_bitmap)
        {
            fprintf(
                stderr,
                "LEAK: iter=%d nunits=%zu bitmap=0x%016lx expected=0x%016lx\n",
                iter,
                nunits,
                nvba->nvba_bitmap,
                initial_bitmap);
        }
        niova_free(raw);
    }
}

static void
test_vbasic_nunits_lt_64(void)
{
    struct niova_vbasic_allocator nvba;
    const size_t unit = 64;
    const size_t requested_nunits = 16;
    const size_t region = unit * requested_nunits;
    int rc;

    rc = niova_vbasic_init_aligned(&nvba, region, unit, unit);
    NIOVA_ASSERT(rc == 0);

    /* IMPORTANT: use allocator-derived nunits */
    const size_t nunits = nvba.nvba_nunits;

    void *ptrs[nunits];

    for (size_t i = 0; i < nunits; i++) {
        rc = niova_vbasic_space_avail(&nvba, unit);
        NIOVA_ASSERT(rc == 0);

        rc = niova_vbasic_malloc(&nvba, unit, &ptrs[i]);
        NIOVA_ASSERT(rc == 0);
        NIOVA_ASSERT(ptrs[i] != NULL);

        size_t map = niova_vbasic_nassigned(&nvba);

        /* bitmap includes non-allocatable bits */
        size_t expected =
            (NIOVA_VBA_MAX_BITS - nunits) + (i + 1);

        NIOVA_ASSERT(map == expected);
    }

    /* After exhausting all allocatable units, space must be unavailable */
    rc = niova_vbasic_space_avail(&nvba, unit);
    NIOVA_ASSERT(rc != 0);
}

static void
test_vbasic_nunits_eq(void)
{
    struct niova_vbasic_allocator nvba;
    const size_t unit = 128;
    const size_t requested_nunits = NIOVA_VBA_MAX_BITS;
    const size_t region = unit * requested_nunits;
    int rc;

    rc = niova_vbasic_init_aligned(&nvba, region, unit, unit);
    NIOVA_ASSERT(rc == 0);

    /* Use allocator-derived nunits */
    const size_t nunits = nvba.nvba_nunits;
    NIOVA_ASSERT(nunits > 0);   /* sanity */

    void *ptrs[nunits];

    /* Allocate all effective units */
    for (size_t i = 0; i < nunits; i++) {
        rc = niova_vbasic_space_avail(&nvba, unit);
        NIOVA_ASSERT(rc == 0);

        rc = niova_vbasic_malloc(&nvba, unit, &ptrs[i]);
        NIOVA_ASSERT(rc == 0);
        NIOVA_ASSERT(ptrs[i] != NULL);

        /*
         * Map semantics:
         * - masked bits may exist
         * - so nassigned() counts masked + allocated
         */
        size_t expected =
            (NIOVA_VBA_MAX_BITS - nunits) + (i + 1);

        NIOVA_ASSERT(niova_vbasic_nassigned(&nvba) == expected);
    }

    /* Full */
    rc = niova_vbasic_space_avail(&nvba, unit);
    NIOVA_ASSERT(rc != 0);
    NIOVA_ASSERT(
        niova_vbasic_nassigned(&nvba) ==
        NIOVA_VBA_MAX_BITS);

    /* Free first and last */
    rc = niova_vbasic_free(&nvba, ptrs[0], unit);
    NIOVA_ASSERT(rc == 0);

    rc = niova_vbasic_free(&nvba, ptrs[nunits - 1], unit);
    NIOVA_ASSERT(rc == 0);

    NIOVA_ASSERT(
        niova_vbasic_nassigned(&nvba) ==
        NIOVA_VBA_MAX_BITS - 2);

    /* Allocate twice */
    void *p;

    rc = niova_vbasic_space_avail(&nvba, unit);
    NIOVA_ASSERT(rc == 0);
    rc = niova_vbasic_malloc(&nvba, unit, &p);
    NIOVA_ASSERT(rc == 0);

    rc = niova_vbasic_space_avail(&nvba, unit);
    NIOVA_ASSERT(rc == 0);
    rc = niova_vbasic_malloc(&nvba, unit, &p);
    NIOVA_ASSERT(rc == 0);

    NIOVA_ASSERT(
        niova_vbasic_nassigned(&nvba) ==
        NIOVA_VBA_MAX_BITS);
}

static void
test_vbasic_nunits_gt_64_capped(void)
{
    struct niova_vbasic_allocator nvba;
    const size_t unit = 256;
    const size_t requested_nunits = 80;
    const size_t region = unit * requested_nunits;
    int rc;

    rc = niova_vbasic_init_aligned(&nvba, region, unit, unit);
    NIOVA_ASSERT(rc == 0);

    void *ptrs[64];

    for (size_t i = 0; i < 64; i++) {
        rc = niova_vbasic_space_avail(&nvba, unit);
        NIOVA_ASSERT(rc == 0);

        rc = niova_vbasic_malloc(&nvba, unit, &ptrs[i]);
        NIOVA_ASSERT(rc == 0);
        NIOVA_ASSERT(ptrs[i] != NULL);

        NIOVA_ASSERT(niova_vbasic_nassigned(&nvba) == i + 1);
    }

    /* Cap must apply */
    rc = niova_vbasic_space_avail(&nvba, unit);
    NIOVA_ASSERT(rc != 0);
    NIOVA_ASSERT(niova_vbasic_nassigned(&nvba) == 64);

    /* Free + reallocate */
    rc = niova_vbasic_free(&nvba, ptrs[10], unit);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(niova_vbasic_nassigned(&nvba) == 63);

    void *p;

    rc = niova_vbasic_space_avail(&nvba, unit);
    NIOVA_ASSERT(rc == 0);

    rc = niova_vbasic_malloc(&nvba, unit, &p);
    NIOVA_ASSERT(rc == 0);

    NIOVA_ASSERT(niova_vbasic_nassigned(&nvba) == 64);
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
    size_t reg_size = 0;
    uint32_t nunits = 0;

    //Test1:

    //vba NULL
    NIOVA_ASSERT(niova_vbasic_init_aligned(NULL, 65536, 4096, 0) == -EINVAL);

    //alignment 0
    NIOVA_ASSERT(niova_vbasic_init_aligned(&tnvba, 65536, 4096, 0) == -EDOM);

    struct niova_vbasic_allocator *nvba =
        niova_malloc(sizeof(struct niova_vbasic_allocator) + 4096);
    NIOVA_ASSERT(nvba);

    //region size zero
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 0, 64, 15) == -ENODATA);

    //alignment should be power of 2
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 64, 15) == -EDOM);

    //alignment < unit size
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 64, 128) == -EOVERFLOW);

    //unit size should be multiple of alignmnent
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 81, 16) == -EDOM);

    //Allow more than 64 num of units but underlying code caps it to 64
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 32, 16) == 0);

    reg_size = nvba->nvba_region_sz;
    nunits = nvba->nvba_nunits;

    //due to region base alignment we might see reduced region size and less
    //nunits as we do not support partial unit
    NIOVA_ASSERT(reg_size > 0 && reg_size <= 4096);
    NIOVA_ASSERT(nunits > 0 && nunits <= 64);

    //check space availability
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, reg_size) == 0);

    //check if all units are free
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == 0);

    memset(nvba, 0, sizeof(*nvba));

    //pass unit size 0, expect max 64 units
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 0, 16) == 0);

    nunits = nvba->nvba_nunits;

    NIOVA_ASSERT(nunits && nunits <= NIOVA_VBA_MAX_BITS);

    memset(nvba, 0, sizeof(*nvba));

    //should succeed
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, UINT32_MAX,
                                           0, 16) == 0);

    reg_size = nvba->nvba_region_sz;
    nunits = nvba->nvba_nunits;

    //should be valid and below set value
    NIOVA_ASSERT(reg_size && reg_size <= UINT32_MAX);
    NIOVA_ASSERT(nunits && nunits <= 64);

    //space should be available
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba,
                                          nunits * nvba->nvba_unit_size) == 0);

    //goes beyond 64 units
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, reg_size + 1024) == -E2BIG);

    memset(nvba, 0, sizeof(*nvba));

     //region smaller than unit size
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 63, 64, 16) == -EINVAL);

    //do not allow more than UINT32_MAX unit size
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, UINT32_MAX * 2,
                                           (size_t)UINT32_MAX + 1,
                                           16) == -EINVAL);
    // Less than 64 nunits supported
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 256, 16) == 0);

    nunits = nvba->nvba_nunits;

    //non-allocatable units has all bits set
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == NIOVA_VBA_MAX_BITS - nunits);

    //space is available in all inited units
    reg_size = nvba->nvba_region_sz;
    nunits = nvba->nvba_nunits;

    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 256 * nunits) == 0);

    void *ptr = NULL;
    //allocate more than inited units
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, 256 * (nunits + 1),
                                     &ptr) == -ENOSPC);

    //reset only nvba fileds, keep region intact
    memset(nvba, 0, sizeof(*nvba));

    //ok
    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, 4096, 64, 16) == 0);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == 0);

    reg_size = nvba->nvba_region_sz;
    nunits = nvba->nvba_nunits;

    ptr = NULL;
    // alloc entire region
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, reg_size, &ptr) == 0);

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

    //Test2:
    reg_size = 16 * 1024;

    nvba = niova_malloc(sizeof(struct niova_vbasic_allocator) + reg_size);
    NIOVA_ASSERT(nvba);

    NIOVA_ASSERT(niova_vbasic_init_aligned(nvba, reg_size, 64, 64) == 0);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, 1) == 0);

    reg_size = nvba->nvba_region_sz;
    nunits = nvba->nvba_nunits;

    NIOVA_ASSERT(reg_size >= nunits * 64);
    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, reg_size + 1) == -E2BIG);
    NIOVA_ASSERT(niova_vbasic_nassigned(nvba) == 0);

    ptr = NULL;
    // alloc entire region
    NIOVA_ASSERT(niova_vbasic_malloc(nvba, nunits * 64, &ptr) == 0);

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
    NIOVA_ASSERT(niova_vbasic_free(nvba, ptr, nunits * 64) == 0);

    NIOVA_ASSERT(niova_vbasic_space_avail(nvba, nunits * 64) == 0);
    niova_free(nvba);
}

int
main(void)
{
    niova_vbasic_alloc_test();
    niova_vbasic_alloc_test2();
    niova_vbasic_alloc_alignment_test();
    test_vbasic_random_stress_full();
    test_vbasic_nunits_lt_64();
    test_vbasic_nunits_eq();
    test_vbasic_nunits_gt_64_capped();

    return 0;
}
