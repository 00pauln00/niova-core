/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "common.h"
#include "log.h"
#include "random.h"

#include "io.h"

#define TEST_MAX_IOVS 64
#define TEST_ITERATIONS 10000
#define SUM_CHUNK_SIZE 16

static int
iov_test_copy_from_iovs(void)
{
#define TC_NIOVS 4
    struct iovec iovs[TC_NIOVS] =
        {
          [0].iov_len = 1, [0].iov_base = (void *)"a",
          [1].iov_len = 2, [1].iov_base = (void *)"bc",
          [2].iov_len = 3, [2].iov_base = (void *)"def",
          [3].iov_len = 4, [3].iov_base = (void *)"ghij",
        };

    char dest[10] = {0};

    ssize_t rc = niova_io_copy_from_iovs(dest, 10, iovs, TC_NIOVS);
    NIOVA_ASSERT(rc == 10);

    for (int i = 0; i < 10; i++)
    {
        NIOVA_ASSERT(dest[i] == 'a' + i);
    }

    return 0;
}

static int
iov_test_copy_from_iovs1(void)
{
#define TC_NIOVS 4
    struct iovec iovs[TC_NIOVS] =
        {
          [0].iov_len = 1, [0].iov_base = (void *)"a",
          [1].iov_len = 2, [1].iov_base = (void *)"bc",
          [2].iov_len = 3, [2].iov_base = (void *)"def",
          [3].iov_len = 4, [3].iov_base = (void *)"ghij",
        };

    char dest[10] = {0};

    // Don't copy into the last byte
    ssize_t rc = niova_io_copy_from_iovs(dest, 9, iovs, TC_NIOVS);
    NIOVA_ASSERT(rc == 9);

    for (int i = 0; i < 9; i++)
    {
        NIOVA_ASSERT(dest[i] == 'a' + i);
    }

    NIOVA_ASSERT(dest[9] == 0);

    return 0;
}

static int
iov_test_basic(void)
{
    struct iovec iov = {0};

    ssize_t rrc = niova_io_iovs_advance(NULL, 1, 0, NULL);
    NIOVA_ASSERT(rrc == -EINVAL);

    rrc = niova_io_iovs_advance(&iov, 1, -1, NULL);
    NIOVA_ASSERT(rrc == -EINVAL);

    rrc = niova_io_iovs_advance(&iov, 1, 1, NULL);
    NIOVA_ASSERT(rrc == -ERANGE);

    iov.iov_len = 2;

    rrc = niova_io_iovs_advance(&iov, 1, 1, NULL);
    NIOVA_ASSERT(rrc == 0 && iov.iov_len == 1 &&
                 (uintptr_t)iov.iov_base == (uintptr_t)1);

    rrc = niova_io_iovs_advance(&iov, 1, 1, &iov);
    NIOVA_ASSERT(rrc == -EFAULT);

    iov.iov_base = NULL;
    iov.iov_len = 2;

    struct iovec save = {(void *)-1ULL, -1UL};
    rrc = niova_io_iovs_advance(&iov, 1, 1, &save);
    NIOVA_ASSERT(rrc == 0 && save.iov_len == 2 &&
                 (uintptr_t)save.iov_base == (uintptr_t)0);

    int rc = niova_io_iov_restore(&iov, 1, 0, &save);
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

    NIOVA_ASSERT(niova_io_iovs_total_size_get(iovs, n) == (n * n));

    // Ensure that iovs in the array are not permitted for iov saving
    for (size_t i = 0; i < n; i++)
        NIOVA_ASSERT(niova_io_iovs_advance(iovs, n, 1, &iovs[i]) == -EFAULT);

    struct iovec save = {0};
    ssize_t idx = niova_io_iovs_advance(iovs, n, 1, &save);
    NIOVA_ASSERT(idx == 0 && save.iov_len == 100 &&
                 save.iov_base == (void *)0 &&
                 iovs[idx].iov_base == (void *)1 &&
                 iovs[idx].iov_len == n - 1);

    NIOVA_ASSERT(niova_io_iovs_total_size_get(&iovs[idx], n) == ((n * n) - 1));

    niova_io_iov_restore(iovs, n, idx, &save);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(iovs, n) == (n * n));

    // Consume whole iovs
    idx = niova_io_iovs_advance(iovs, n, 4 * n, &save);
    NIOVA_ASSERT(idx == 4 && save.iov_len == 100 &&
                 save.iov_base == (void *)400 &&
                 iovs[idx].iov_base == (void *)400 &&
                 iovs[idx].iov_len == 100);

    NIOVA_ASSERT(niova_io_iovs_total_size_get(&iovs[idx], n - idx) ==
                 (n * (n - idx)));

    niova_io_iov_restore(iovs, n, idx, &save);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(iovs, n) == (n * n));

    // Consume everything in one shot:  niova-block issue #141
    idx = niova_io_iovs_advance(iovs, n, n * n, NULL);
    NIOVA_ASSERT(idx == -EXFULL);

    off_t off = 0;
    size_t niovs = n;

    // Test consume and restore
    for (size_t i = 0; i < (n + (n / 3)); i++)
    {
        off += i;

        size_t start_idx = 0;
        start_idx = niova_io_iovs_advance(&iovs[start_idx], niovs, off, &save);

        SIMPLE_LOG_MSG(LL_DEBUG, "i=%zu off=%ld start_idx=%zu",
                       i, off, start_idx);

        NIOVA_ASSERT(start_idx == off / n);
        NIOVA_ASSERT(niova_io_iovs_total_size_get(
                         &iovs[start_idx], (niovs - start_idx)) ==
                         (n * n) - off);

        NIOVA_ASSERT(!niova_io_iov_restore(iovs, n, start_idx, &save));
        NIOVA_ASSERT(niova_io_iovs_total_size_get(iovs, n) == (n * n));
    }

    return 0;
}

static int
iov_test_num_to_meet_size2(void)
{
    NIOVA_ASSERT(
        niova_io_iovs_num_to_meet_size2(NULL, 0, 0, 0, NULL, NULL) == -EINVAL);

    struct iovec iov = {0};
    NIOVA_ASSERT(
        niova_io_iovs_num_to_meet_size2(&iov, 0, 0, 0, NULL, NULL) == -EINVAL);

    NIOVA_ASSERT(
        niova_io_iovs_num_to_meet_size2(&iov, 1, 0, 0, 0, NULL) == -EINVAL);

    off_t ret_idx = -1;

    iov.iov_len = 2;
    size_t prune_cnt = 0;

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     &iov, 1, 1, 0, &ret_idx, &prune_cnt) == 1);
    NIOVA_ASSERT(prune_cnt == 1);
    FATAL_IF(ret_idx != 0, "ret_idx=%ld", ret_idx);

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     &iov, 1, 1, 1, &ret_idx, &prune_cnt) == 1);
    FATAL_IF(prune_cnt != 0, "prune_cnt=%lu", prune_cnt);
    FATAL_IF(ret_idx != 0, "ret_idx=%ld", ret_idx);

    // bytes_already_consumed + requested_size exceeds total
    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     &iov, 1, 1, 2, &ret_idx, &prune_cnt) == -EOVERFLOW);

    size_t iovX_len = 10;
    struct iovec iovX[2] = {[0].iov_len = 10, [1].iov_len = 10};

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     iovX, 2, 1, 0, &ret_idx, &prune_cnt) == 1);
    FATAL_IF(prune_cnt != 9, "prune_cnt=%lu", prune_cnt);
    FATAL_IF(ret_idx != 0, "ret_idx=%ld", ret_idx);

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     iovX, 2, 1, 8, &ret_idx, &prune_cnt) == 1);
    FATAL_IF(prune_cnt != 1, "prune_cnt=%lu", prune_cnt);
    FATAL_IF(ret_idx != 0, "ret_idx=%ld", ret_idx);

    // Move to the 2nd byte in the 1st iov
    size_t r = iovX_len - (iovX_len - 1);
    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     iovX, 2, 1, 0, &ret_idx, &prune_cnt) == 1);
    FATAL_IF(prune_cnt != (iovX_len - 1), "prune_cnt=%lu", prune_cnt);
    FATAL_IF(ret_idx != 0, "ret_idx=%ld", ret_idx);

    // Move to the 2nd byte in the 2nd iov
    r = iovX_len + 1;
    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size2(
                     iovX, 2, r, 0, &ret_idx, &prune_cnt) == 2);
    FATAL_IF(prune_cnt != iovX_len - 1, "prune_cnt=%lu", prune_cnt);
    FATAL_IF(ret_idx != 0, "ret_idx=%ld", ret_idx);

    int rc = niova_io_iovs_num_to_meet_size2(iovX, 2, 1, (2 * iovX_len) - 1,
                                             &ret_idx, &prune_cnt);
    FATAL_IF(rc != 1, "rc=%d", rc);
    FATAL_IF(prune_cnt != 0, "prune_cnt=%lu", prune_cnt);
    FATAL_IF(ret_idx != 1, "ret_idx=%ld", ret_idx);

    return 0;
}

static int
iov_test_num_to_meet_size(void)
{
    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(NULL, 0, 0, NULL) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(NULL, 0, 0) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_map_consumed(NULL, NULL, 0, 0, 0) == -EINVAL);


    struct iovec iov = {0};
    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(&iov, 0, 0, NULL) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(&iov, 0, 0) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_map_consumed(&iov, NULL, 0, 0, 0) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_map_consumed(&iov, &iov, 1, 0, 0) == -EINVAL);

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(&iov, 1, 0, NULL) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(&iov, 1, 0) == 1);

    iov.iov_len = 2;
    size_t prune_cnt = 0;

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(&iov, 1, 0, NULL) == -EINVAL);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(&iov, 1, 0) == 0);

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(&iov, 1, 1, &prune_cnt) == 1 &&
                 prune_cnt == 1);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(&iov, 1, 1) == 0);

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(&iov, 1, 2, &prune_cnt) == 1 &&
                 prune_cnt == 0);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(&iov, 1, 2) == 1);

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(&iov, 1, 3, NULL) ==
                 -EOVERFLOW);
    NIOVA_ASSERT(niova_io_iovs_num_already_consumed(&iov, 1, 3) == 1);


    struct iovec iovs[33] = {0};
    for (int i = 0; i < 33; i++)
        iovs[i].iov_len = 1ULL << i;

    NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(
                     iovs, 33, 1ULL << 34, NULL) ==
                 -EOVERFLOW);
    NIOVA_ASSERT(
        niova_io_iovs_num_already_consumed(iovs, 33, 1ULL << 34) == 33);

    for (int i = 0; i < 33; i++)
    {
        NIOVA_ASSERT(niova_io_iovs_num_to_meet_size(
                         iovs, 33, 1ULL << i, &prune_cnt) == i + 1);
        NIOVA_ASSERT(prune_cnt == ((1ULL << i) - 1));

//        fprintf(stderr, "niova_io_iovs_num_already_consumed(%d) = %zd\n",
//                i, niova_io_iovs_num_already_consumed(iovs, 33, 1ULL << i));

        NIOVA_ASSERT(
            niova_io_iovs_num_already_consumed(iovs, 33, 1ULL << i) ==
            MAX(1, i));
    }

    return 0;
}

static int
iov_test_map_consumed(void)
{
    struct iovec iovA[2] = { [0].iov_len = 1, [1].iov_len = 1 };
    struct iovec iovB[2] = {0};

    NIOVA_ASSERT(niova_io_iovs_map_consumed(iovA, &iovA[1], 2, 0, 0) ==
                 -EINVAL);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    NIOVA_ASSERT(niova_io_iovs_map_consumed(iovA, iovA - 1, 2, 0, 0) ==
                 -EINVAL);
#pragma GCC diagnostic pop

    NIOVA_ASSERT(niova_io_iovs_map_consumed(iovA, iovB, 2, 0, 3) ==
                 -EOVERFLOW);
    NIOVA_ASSERT(niova_io_iovs_map_consumed(iovA, iovB, 2, 1, 0) == 1);

    struct iovec iovs[33] = {0};
    for (int i = 0; i < 33; i++)
        iovs[i].iov_len = 1ULL << i;

    struct iovec dest[33] = {0};
    ssize_t niovs = niova_io_iovs_map_consumed(iovs, dest, 33, 0, -1UL);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) ==
                 niova_io_iovs_total_size_get(iovs, niovs));

    niovs = niova_io_iovs_map_consumed(iovs, dest, 33, 0, 1);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1);

    niovs = niova_io_iovs_map_consumed(iovs, dest, 33, 0, 1000);
    NIOVA_ASSERT(niovs == 10);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1000);

    niovs = niova_io_iovs_map_consumed(iovs, dest, 33, 0, 1000000);
    NIOVA_ASSERT(niovs == 20);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1000000);

    niovs = niova_io_iovs_map_consumed(iovs, dest, 33, 0, 1000000000);
    NIOVA_ASSERT(niovs == 30);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1000000000);

    for (int i = 0; i < 33; i++)
    {
        niovs = niova_io_iovs_map_consumed(iovs, dest, 33, 0, 1ULL << i);
//        fprintf(stderr,
//                "niova_io_iovs_map_consumed(%d) niovs=%zd total-size=%zd\n",
//                i, niovs, niova_io_iovs_total_size_get(dest, niovs));

        NIOVA_ASSERT(niovs == i + 1);
        NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1ULL << i);
    }

    return 0;
}

static int
iov_test_map_consumed2(void)
{
    struct iovec iovA[2] = { [0].iov_len = 1, [1].iov_len = 1 };
    struct iovec iovB[2] = {0};

    NIOVA_ASSERT(niova_io_iovs_map_consumed2(iovA, &iovA[1], 2, 2, 0, 0) ==
                 -EINVAL);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    NIOVA_ASSERT(niova_io_iovs_map_consumed2(iovA, iovA - 1, 2, 2, 0, 0) ==
                 -EINVAL);
#pragma GCC diagnostic pop

    NIOVA_ASSERT(niova_io_iovs_map_consumed2(iovA, iovB, 2, 2, 0, 3) ==
                 -EOVERFLOW);
    NIOVA_ASSERT(niova_io_iovs_map_consumed2(iovA, iovB, 2, 2, 1, 0) == 1);

    struct iovec iovs[33] = {0};
    uint64_t total_size = 0;
    for (int i = 0; i < 33; i++)
    {
        iovs[i].iov_len = 1ULL << i;
        total_size += iovs[i].iov_len;
    }

    struct iovec dest[33] = {0};
    ssize_t niovs = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, -1UL);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) ==
                 niova_io_iovs_total_size_get(iovs, niovs));
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == total_size);

    ssize_t rc = niova_io_iovs_map_consumed2(iovs, dest, 33, 32, 0, -1UL);
    NIOVA_ASSERT(rc == -EOVERFLOW);

    rc = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, total_size + 1);
    NIOVA_ASSERT(rc == -EOVERFLOW);

    niovs = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, 1);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1);

    niovs = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, 1000);
    NIOVA_ASSERT(niovs == 10);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1000);

    niovs = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, 1000000);
    NIOVA_ASSERT(niovs == 20);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1000000);

    niovs = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, 1000000000);
    NIOVA_ASSERT(niovs == 30);
    NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1000000000);

    for (int i = 0; i < 33; i++)
    {
        niovs = niova_io_iovs_map_consumed2(iovs, dest, 33, 33, 0, 1ULL << i);
//        fprintf(stderr,
//                "niova_io_iovs_map_consumed(%d) niovs=%zd total-size=%zd\n",
//                i, niovs, niova_io_iovs_total_size_get(dest, niovs));

        NIOVA_ASSERT(niovs == i + 1);
        NIOVA_ASSERT(niova_io_iovs_total_size_get(dest, niovs) == 1ULL << i);
    }

    return 0;
}

static int
iov_test_iovs_memset(void)
{
    struct iovec iovs[TC_NIOVS];
    char *cptr[TC_NIOVS];

    for (int i = 0; i < TC_NIOVS; i++)
    {
        iovs[i].iov_base = malloc(i + 1);
        iovs[i].iov_len = i + 1;
        memset(iovs[i].iov_base, i + 1, i + 1);

        cptr[i] = (char *)iovs[i].iov_base;
    }

    // Memset the first 2 iovs entirely and the first byte of the 3rd
    ssize_t rc = niova_io_memset_iovs(iovs, TC_NIOVS, 0, 4);
    NIOVA_ASSERT(rc == 4);

    NIOVA_ASSERT(cptr[0][0] == 0);
    NIOVA_ASSERT(cptr[1][0] == 0);
    NIOVA_ASSERT(cptr[1][1] == 0);
    NIOVA_ASSERT(cptr[2][0] == 0);
    NIOVA_ASSERT(cptr[2][1] == 3);
    NIOVA_ASSERT(cptr[3][0] == 4);

    for (int i = 0; i < TC_NIOVS; i++)
        free(iovs[i].iov_base);

    return 0;
}

/**
 * Helper to allocate and initialize iovs with random sizes from a buffer
 */
static struct iovec *
allocate_and_init_iovs(const unsigned char *buffer, size_t buffer_size,
                       size_t min_size, size_t max_size, size_t *out_niovs)
{
    struct iovec *iovs = malloc(TEST_MAX_IOVS * sizeof(struct iovec));
    NIOVA_ASSERT(iovs != NULL);

    size_t niovs = 0;
    size_t offset = 0;

    while (offset < buffer_size && niovs < TEST_MAX_IOVS)
    {
        size_t remaining = buffer_size - offset;
        size_t iov_size;

        if (remaining <= min_size || niovs == TEST_MAX_IOVS - 1)
        {
            iov_size = remaining;
        }
        else
        {
            size_t max_iov_size = MIN(max_size, remaining);
            iov_size =
                min_size + (random_get() % (max_iov_size - min_size + 1));
        }

        unsigned char *iov_buffer = malloc(iov_size);
        NIOVA_ASSERT(iov_buffer != NULL);

        memcpy(iov_buffer, buffer + offset, iov_size);

        iovs[niovs].iov_base = iov_buffer;
        iovs[niovs].iov_len = iov_size;

        offset += iov_size;
        niovs++;
    }

    *out_niovs = niovs;
    return iovs;
}

/**
 * Helper to free iovs allocated by allocate_and_init_iovs
 */
static void
free_iovs(struct iovec *iovs, size_t niovs)
{
    for (size_t i = 0; i < niovs; i++)
        free(iovs[i].iov_base);
    free(iovs);
}

/**
 * Context for read test callback
 */
struct read_test_ctx
{
    unsigned char *dest_buffer;
    size_t offset;
    uint32_t *sum_array;  // Array to store sum of each 16-byte chunk
    size_t sum_count;     // Number of sums computed
    uint32_t current_sum; // Current sum being accumulated
    size_t bytes_in_sum;  // Bytes accumulated in current sum
};

/**
 * Callback to read data from iovs into a buffer and compute sums
 */
static int
read_callback(const void *data, size_t len, void *user_ctx)
{
    struct read_test_ctx *ctx = (struct read_test_ctx *)user_ctx;

    const unsigned char *bytes = (const unsigned char *)data;

    // Copy data to destination buffer
    memcpy(ctx->dest_buffer + ctx->offset, data, len);

    // Compute sums of SUM_CHUNK_SIZE consecutive bytes
    for (size_t i = 0; i < len; i++)
    {
        ctx->current_sum += bytes[i];
        ctx->bytes_in_sum++;

        if (ctx->bytes_in_sum == SUM_CHUNK_SIZE)
        {
            ctx->sum_array[ctx->sum_count++] = ctx->current_sum;
            ctx->current_sum = 0;
            ctx->bytes_in_sum = 0;
        }
    }

    ctx->offset += len;

    return 0;
}

/**
 * Context for write test callback
 */
struct write_test_ctx
{
    const unsigned char *src_buffer;
    size_t offset;
};

/**
 * Callback to write data from buffer into iovs
 */
static int
write_callback(const void *data, size_t len, void *user_ctx)
{
    struct write_test_ctx *ctx = (struct write_test_ctx *)user_ctx;

    memcpy((void *)data, ctx->src_buffer + ctx->offset, len);
    ctx->offset += len;

    return 0;
}

/**
 * Test niova_io_iterate_iovs for reading from iovs
 */
static int
iov_test_iterate_read(void)
{
    int failures = 0;

    for (int iter = 0; iter < TEST_ITERATIONS; iter++)
    {
        // Generate buffer with random values
        // Make it a multiple of SUM_CHUNK_SIZE for sum computation
        size_t buffer_size = 256 + (random_get() % (8192 - 256 + 1));
        buffer_size = (buffer_size / SUM_CHUNK_SIZE) * SUM_CHUNK_SIZE;

        unsigned char *src_buffer = malloc(buffer_size);
        NIOVA_ASSERT(src_buffer != NULL);

        for (size_t i = 0; i < buffer_size; i++)
            src_buffer[i] = (unsigned char)(random_get() & 0xFF);

        // Compute expected sums from source buffer
        size_t num_sums = buffer_size / SUM_CHUNK_SIZE;
        uint32_t *expected_sums = malloc(num_sums * sizeof(uint32_t));
        NIOVA_ASSERT(expected_sums != NULL);

        for (size_t i = 0; i < num_sums; i++)
        {
            uint32_t sum = 0;
            for (size_t j = 0; j < SUM_CHUNK_SIZE; j++)
                sum += src_buffer[i * SUM_CHUNK_SIZE + j];
            expected_sums[i] = sum;
        }

        // (16 < iov_sizes < 512)
        size_t min_iov = 16;
        size_t max_iov = 512;

        size_t niovs;
        struct iovec *iovs = allocate_and_init_iovs(src_buffer, buffer_size,
                                                    min_iov, max_iov, &niovs);

        unsigned char *dest_buffer = calloc(1, buffer_size);
        NIOVA_ASSERT(dest_buffer != NULL);

        uint32_t *computed_sums = calloc(num_sums, sizeof(uint32_t));
        NIOVA_ASSERT(computed_sums != NULL);

        struct read_test_ctx ctx = {.dest_buffer = dest_buffer,
                                    .offset = 0,
                                    .sum_array = computed_sums,
                                    .sum_count = 0,
                                    .current_sum = 0,
                                    .bytes_in_sum = 0};

        ssize_t rc = niova_io_iterate_iovs(iovs, niovs, buffer_size,
                                           read_callback, &ctx);

        if (rc != (ssize_t)buffer_size)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "iter %d: Expected %zu bytes, got %zd",
                           iter, buffer_size, rc);
            failures++;
        }

        // Verify data matches
        if (memcmp(src_buffer, dest_buffer, buffer_size) != 0)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "iter %d: Data mismatch", iter);
            failures++;
        }

        // Verify sums match
        if (ctx.sum_count != num_sums)
        {
            SIMPLE_LOG_MSG(LL_ERROR,
                           "iter %d: Sum count mismatch: expected %zu, got %zu",
                           iter, num_sums, ctx.sum_count);
            failures++;
        }
        else
        {
            if (memcmp(computed_sums, expected_sums,
                       num_sums * sizeof(uint32_t)) != 0)
            {
            SIMPLE_LOG_MSG(LL_ERROR, "iter %d: Sums mismatch", iter);
            failures++;
            }
        }

        free(src_buffer);
        free(dest_buffer);
        free(expected_sums);
        free(computed_sums);
        free_iovs(iovs, niovs);
    }

    return failures;
}

/**
 * Test niova_io_iterate_iovs for writing to iovs
 */
static int
iov_test_iterate_write(void)
{
    int failures = 0;

    for (int iter = 0; iter < TEST_ITERATIONS; iter++)
    {
        // Generate buffer with random values
        size_t buffer_size = 256 + (random_get() % (8192 - 256 + 1));

        unsigned char *src_buffer = malloc(buffer_size);
        NIOVA_ASSERT(src_buffer != NULL);

        for (size_t i = 0; i < buffer_size; i++)
            src_buffer[i] = (unsigned char)(random_get() & 0xFF);

        // (16 < iov_sizes < 512)
        size_t min_iov = 16;
        size_t max_iov = 512;

        size_t niovs;
        struct iovec *temp_iovs = allocate_and_init_iovs(
            src_buffer, buffer_size, min_iov, max_iov, &niovs);

        struct write_test_ctx ctx = {
            .src_buffer = src_buffer, .offset = 0};

        ssize_t rc = niova_io_iterate_iovs(temp_iovs, niovs, buffer_size,
                                           write_callback, &ctx);

        if (rc != (ssize_t)buffer_size)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "iter %d: Expected %zu bytes, got %zd",
                           iter, buffer_size, rc);
            failures++;
        }

        // Reconstruct buffer from iovs and verify
        unsigned char *verify_buffer = malloc(buffer_size);
        NIOVA_ASSERT(verify_buffer != NULL);

        size_t offset = 0;
        for (size_t i = 0; i < niovs; i++)
        {
            memcpy(verify_buffer + offset, temp_iovs[i].iov_base,
                   temp_iovs[i].iov_len);
            offset += temp_iovs[i].iov_len;
        }

        if (memcmp(src_buffer, verify_buffer, buffer_size) != 0)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "iter %d: Data mismatch after write",
                           iter);
            failures++;
        }

        free(src_buffer);
        free(verify_buffer);
        free_iovs(temp_iovs, niovs);
    }

    return failures;
}

/**
 * Test specific cases and behaviors of niova_io_iterate_iovs
 */
static int
iov_test_iterate_cases(void)
{
    int failures = 0;

    ssize_t rc;

    // Partial read (num_bytes < total iov size)
    unsigned char buffer[100];
    for (size_t i = 0; i < 100; i++)
        buffer[i] = (unsigned char)i;

    struct iovec iovs[3] = {{.iov_base = buffer, .iov_len = 30},
                            {.iov_base = buffer + 30, .iov_len = 30},
                            {.iov_base = buffer + 60, .iov_len = 40}};

    unsigned char dest[50];
    uint32_t sums[10];
    struct read_test_ctx ctx = {.dest_buffer = dest,
                                .offset = 0,
                                .sum_array = sums,
                                .sum_count = 0,
                                .current_sum = 0,
                                .bytes_in_sum = 0};

    rc = niova_io_iterate_iovs(iovs, 3, 50, read_callback, &ctx);
    if (rc != 50)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "Partial read expected 50 bytes, got %zd", rc);
        failures++;
    }

    if (memcmp(buffer, dest, 50) != 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "Partial read data mismatch");
        failures++;
    }

    // SIZE_MAX should iterate over everything
    unsigned char dest4[100];
    uint32_t sums4[10];
    struct read_test_ctx ctx4 = {.dest_buffer = dest4,
                                 .offset = 0,
                                 .sum_array = sums4,
                                 .sum_count = 0,
                                 .current_sum = 0,
                                 .bytes_in_sum = 0};

    rc = niova_io_iterate_iovs(iovs, 3, SIZE_MAX, read_callback, &ctx4);
    if (rc != 100)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "SIZE_MAX read expected 100 bytes, got %zd",
                       rc);
        failures++;
    }

    if (memcmp(buffer, dest4, 100) != 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "SIZE_MAX read data mismatch");
        failures++;
    }

    return failures;
}

int
main(void)
{
    unsigned int seed = (unsigned int)time(NULL);
    random_init(seed);

    NIOVA_ASSERT(!iov_test_basic());
    NIOVA_ASSERT(!iov_test());
    NIOVA_ASSERT(!iov_test_num_to_meet_size());
    NIOVA_ASSERT(!iov_test_num_to_meet_size2());
    NIOVA_ASSERT(!iov_test_map_consumed());
    NIOVA_ASSERT(!iov_test_map_consumed2());
    NIOVA_ASSERT(!iov_test_copy_from_iovs());
    NIOVA_ASSERT(!iov_test_copy_from_iovs1());
    NIOVA_ASSERT(!iov_test_iovs_memset());
    NIOVA_ASSERT(!iov_test_iterate_cases());
    NIOVA_ASSERT(!iov_test_iterate_read());
    NIOVA_ASSERT(!iov_test_iterate_write());

    return 0;
}