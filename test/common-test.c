/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */

#include "common.h"
#include "log.h"
#include "random.h"

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

int
main(void)
{
    highest_power_of_two_test();
    ssize_t_checks();

    return 0;
}
