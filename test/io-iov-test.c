/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */
#include "common.h"
#include "log.h"

#include "io.h"

static int
iov_test_basic(void)
{
    struct iovec iov = {0};

    ssize_t rrc = io_iovs_advance(NULL, 1, 0, NULL);
    NIOVA_ASSERT(rrc == -EINVAL);

    rrc = io_iovs_advance(&iov, 1, -1, NULL);
    NIOVA_ASSERT(rrc == -EINVAL);

    rrc = io_iovs_advance(&iov, 1, 1, NULL);
    NIOVA_ASSERT(rrc == -ERANGE);

    iov.iov_len = 2;

    rrc = io_iovs_advance(&iov, 1, 1, NULL);
    NIOVA_ASSERT(rrc == 0 && iov.iov_len == 1 &&
                 (uintptr_t)iov.iov_base == (uintptr_t)1);

    rrc = io_iovs_advance(&iov, 1, 1, &iov);
    NIOVA_ASSERT(rrc == -EFAULT);

    iov.iov_base = NULL;
    iov.iov_len = 2;

    struct iovec save = {(void *)-1ULL, -1UL};
    rrc = io_iovs_advance(&iov, 1, 1, &save);
    NIOVA_ASSERT(rrc == 0 && save.iov_len == 2 &&
                 (uintptr_t)save.iov_base == (uintptr_t)0);

    int rc = io_iov_restore(&iov, 1, 0, &save);
    NIOVA_ASSERT(rc == 0 && iov.iov_len == 2 &&
                 (uintptr_t)iov.iov_base == (uintptr_t)0);

    return 0;
}

static int
iov_test(void)
{
    size_t n = 100;
    struct iovec iovs[n];

    for (size_t i = 0; i < n; i++)
    {
        iovs[i].iov_base = (void *)(uintptr_t)(i * n);
        iovs[i].iov_len = n;
    }

    NIOVA_ASSERT(io_iovs_total_size_get(iovs, n) == (n * n));

    // Ensure that iovs in the array are not permitted for iov saving
    for (size_t i = 0; i < n; i++)
        NIOVA_ASSERT(io_iovs_advance(iovs, n, 1, &iovs[i]) == -EFAULT);

    struct iovec save = {0};
    ssize_t idx = io_iovs_advance(iovs, n, 1, &save);
    NIOVA_ASSERT(idx == 0 && save.iov_len == 100 &&
                 save.iov_base == (void *)0 &&
                 iovs[idx].iov_base == (void *)1 &&
                 iovs[idx].iov_len == n - 1);

    NIOVA_ASSERT(io_iovs_total_size_get(&iovs[idx], n) == ((n * n) - 1));

    io_iov_restore(iovs, n, idx, &save);
    NIOVA_ASSERT(io_iovs_total_size_get(iovs, n) == (n * n));

    // Consume whole iovs
    idx = io_iovs_advance(iovs, n, 4 * n, &save);
    NIOVA_ASSERT(idx == 4 && save.iov_len == 100 &&
                 save.iov_base == (void *)400 &&
                 iovs[idx].iov_base == (void *)400 &&
                 iovs[idx].iov_len == 100);

    NIOVA_ASSERT(io_iovs_total_size_get(&iovs[idx], n - idx) ==
                 (n * (n - idx)));

    io_iov_restore(iovs, n, idx, &save);
    NIOVA_ASSERT(io_iovs_total_size_get(iovs, n) == (n * n));

    off_t off = 0;
    size_t niovs = n;

    // Test consume and restore
    for (size_t i = 0; i < (n + (n / 3)); i++)
    {
        off += i;

        size_t start_idx = 0;
        start_idx = io_iovs_advance(&iovs[start_idx], niovs, off, &save);

        SIMPLE_LOG_MSG(LL_DEBUG, "i=%zu off=%ld start_idx=%zu",
                       i, off, start_idx);

        NIOVA_ASSERT(start_idx == off / n);
        NIOVA_ASSERT(io_iovs_total_size_get(
                         &iovs[start_idx], (niovs - start_idx)) ==
                         (n * n) - off);

        NIOVA_ASSERT(!io_iov_restore(iovs, n, start_idx, &save));
        NIOVA_ASSERT(io_iovs_total_size_get(iovs, n) == (n * n));
    }

    return 0;
}

int
main(void)
{
    NIOVA_ASSERT(!iov_test_basic());
    NIOVA_ASSERT(!iov_test());

    return 0;
}
