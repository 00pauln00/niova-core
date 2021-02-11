/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */

#include "common.h"
#include "log.h"
#include "random.h"

static void
highest_set_bit_pos_from_val_test(void)
{
    for (int i = 0; i < TYPE_SZ_BITS(unsigned long long); i++)
    {
        const unsigned long long val = 1ULL << i;
        NIOVA_ASSERT(i + 1 == highest_set_bit_pos_from_val(val));

        for (int j = 0; j < 10; j++)
        {
            const unsigned long long val_with_inner_bits =
                val + (random_get() % val);

            NIOVA_ASSERT(i + 1 ==
                         highest_set_bit_pos_from_val(val_with_inner_bits));
        }
    }
}


static void
highest_power_of_two_test(void)
{
    for (int i = 0; i < TYPE_SZ_BITS(unsigned long long); i++)
    {
        const unsigned long long val = 1ULL << i;

        NIOVA_ASSERT(val == highest_power_of_two_from_val(val));

        for (int j = 0; j < 10; j++)
        {
            const unsigned long long val_with_inner_bits =
                val + (random_get() % val);

            NIOVA_ASSERT(val ==
                         highest_power_of_two_from_val(val_with_inner_bits));
        }
    }
}

static void
ssize_t_checks(void)
{
    ssize_t x = -ENOENT;
    int y = -ENOENT;
    NIOVA_ASSERT(x == y);
    NIOVA_ASSERT((ssize_t)x == (ssize_t)y);
    NIOVA_ASSERT(    (int)x == (int)y);
    NIOVA_ASSERT((ssize_t)x == (int)y);

    x = -ENOLCK;
    y = -ENOLCK;
    NIOVA_ASSERT(x == y);
    NIOVA_ASSERT((ssize_t)x == (ssize_t)y);
    NIOVA_ASSERT(    (int)x == (int)y);
    NIOVA_ASSERT((ssize_t)x == (int)y);
}

static void
assign_nbits_test(void)
{
    uint64_t field = 0;
    NIOVA_ASSERT(nconsective_bits_assign(&field, 1) == 0 &&
                 field == 1);

    // Try to release more than were set
    NIOVA_ASSERT(nconsective_bits_release(&field, 0, 2) == -EBADSLT);

    // Perform a valid release
    NIOVA_ASSERT(nconsective_bits_release(&field, 0, 1) == 0);
    NIOVA_ASSERT(field == 0);

    // Try to release a known unset position
    NIOVA_ASSERT(nconsective_bits_release(&field, 63, 1) == -EBADSLT);

    //EINVAL checks
    NIOVA_ASSERT(nconsective_bits_release(&field, 64, 1) == -EINVAL);
    NIOVA_ASSERT(nconsective_bits_release(&field, 0, 0) == -EINVAL);
    NIOVA_ASSERT(nconsective_bits_release(NULL, 0, 1) == -EINVAL);
    NIOVA_ASSERT(nconsective_bits_assign(NULL, 1) == -EINVAL);
    NIOVA_ASSERT(nconsective_bits_assign(&field, 0) == -EINVAL);
    NIOVA_ASSERT(nconsective_bits_assign(&field, 65) ==
                 -EINVAL);

    field = 0;
    NIOVA_ASSERT(nconsective_bits_assign(&field, 64) == 0);
    NIOVA_ASSERT(field == -1ULL);

    field = -1ULL;
    NIOVA_ASSERT(nconsective_bits_release(&field, 0, 64) == 0);
    NIOVA_ASSERT(field == 0);

    field = -1ULL; // reset field
    for (int i = 1; i < 65; i++)
    {
        NIOVA_ASSERT(nconsective_bits_assign(&field, i) ==
                     -ENOSPC);
    }
    field = 0x5555555555555555ULL; //0101
    for (int i = 1; i < 65; i++)
    {
        NIOVA_ASSERT(nconsective_bits_assign(&field, 2) ==
                     -ENOSPC);
    }
    field = 0x5555555555555555ULL; //0101
    for (int i = 1; i < 65; i++)
    {
        int rc = nconsective_bits_assign(&field, 1);
        if (i > 32)
            NIOVA_ASSERT(rc == -ENOSPC && field == -1ULL);
    }

    field = 0x5555555555555555ULL; //0101
    for (int i = 0; i < 32; i++)
        NIOVA_ASSERT(nconsective_bits_release(&field, i * 2, 1) == 0);
    NIOVA_ASSERT(field == 0);

    field = 0xAAAAAAAAAAAAAAAAULL; //1010
    for (int i = 1; i < 65; i++)
    {
        NIOVA_ASSERT(nconsective_bits_assign(&field, 2) ==
                     -ENOSPC);
    }

    field = 0x9249249249249249ULL; //100100
    for (int i = 1; i < 65; i++)
    {
        NIOVA_ASSERT(nconsective_bits_assign(&field, 3) ==
                     -ENOSPC);
    }

    field = 0x8888888888888888ULL; //10001000
    for (int i = 1; i < 65; i++)
    {
        NIOVA_ASSERT(nconsective_bits_assign(&field, 4) ==
                     -ENOSPC);
    }

    // Unset the bits according to the pattern
    field = 0x8888888888888888ULL; //10001000
    for (int i = 1; i <= 16; i++)
        NIOVA_ASSERT(nconsective_bits_release(&field, i * 4 - 1, 1) == 0);
    NIOVA_ASSERT(field == 0);


    // Assign in sets of 2
    field = 0x8888888888888888ULL; //10001000
    for (int i = 1; i < 65; i++)
    {
        int rc = nconsective_bits_assign(&field, 2);
        if (i > 16)                                //10111011
            NIOVA_ASSERT(rc == -ENOSPC && field == 0xbbbbbbbbbbbbbbbb);
    }

    // Assign in sets of 3
    field = 0x8888888888888888ULL; //10001000
    for (int i = 1; i < 65; i++)
    {
        int rc = nconsective_bits_assign(&field, 3);
        if (i > 16)
            NIOVA_ASSERT(rc == -ENOSPC && field == -1ULL);
    }

    // Try to allocate all of the bits
    field = 0x8888888888888888ULL; //10001000
    for (int i = 1; i < 65; i++)
    {
        // After iteration 16 we should have 0xbbbbbbbbbbbbbbbb
        // At iteration 33, the field should empty.
        int rc = (i <= 16 ?
                  nconsective_bits_assign(&field, 2) :
                  nconsective_bits_assign(&field, 1));
        NIOVA_ASSERT(i > 32 ? (rc == -ENOSPC) : (rc >= 0 && rc < 64));
    }
    NIOVA_ASSERT(field == -1ULL);
    field = 0xEEEEEEEEEEEEEEEEULL; //1110
    for (int i = 0; i < 16; i++)
    {
        NIOVA_ASSERT(nconsective_bits_release(&field, 1 + i * 4, 3) == 0);
    }
    NIOVA_ASSERT(field == 0);
}

int
main(void)
{
    assign_nbits_test();
    highest_set_bit_pos_from_val_test();
    highest_power_of_two_test();
    ssize_t_checks();

    return 0;
}
