/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _NIOVA_IO_H_
#define _NIOVA_IO_H_ 1

#include <errno.h>
#include <sys/uio.h>
#include <sys/socket.h>

#define IO_MAX_IOVS UIO_MAXIOV

ssize_t
niova_io_read(int fd, char *buf, size_t size);

ssize_t
niova_io_pwrite(int fd, const char *buf, size_t size, off_t offset);

ssize_t
niova_io_pread(int fd, char *buf, size_t size, off_t offset);

int
niova_io_ftruncate(int fd, off_t length);

int
niova_io_fsync(int fd);

ssize_t
niova_io_fd_drain(int fd, size_t *ret_data);

static inline size_t
niova_io_iovs_total_size_get(const struct iovec *iovs, const size_t iovlen)
{
    size_t total_size = 0;

    if (iovs)
        for (size_t i = 0; i < iovlen; i++)
            total_size += iovs[i].iov_len;

    return total_size;
}

static inline ssize_t
niova_io_iovs_num_already_consumed(const struct iovec *iovs,
                                   const size_t iovlen,
                                   size_t bytes_already_consumed)
{
    if (!iovs || !iovlen)
        return -EINVAL;

    ssize_t my_already_consumed = bytes_already_consumed;
    size_t i;

    for (i = 0; i < iovlen; i++)
    {
        my_already_consumed -= iovs[i].iov_len;
        if (my_already_consumed < 0)
            break;
    }

    return i;
}

static inline ssize_t
niova_io_iovs_num_to_meet_size(const struct iovec *iovs, const size_t iovlen,
                               size_t requested_size, size_t *prune_cnt)
{
    if (!iovs || !iovlen)
        return -EINVAL;

    size_t tally = 0;

    for (size_t i = 0; i < iovlen; i++)
    {
        tally += iovs[i].iov_len;
        if (tally >= requested_size)
        {
            if (prune_cnt)
                *prune_cnt = tally - requested_size;

            return (ssize_t)(i + 1);
        }
    }

    return -EOVERFLOW;
}


ssize_t
niova_io_iovs_map_consumed(const struct iovec *src, struct iovec *dest,
                           const size_t num_iovs,
                           size_t bytes_already_consumed);

ssize_t
niova_io_copy_to_iovs(const char *src, size_t src_size,
                      struct iovec *dest_iovs,
                      const size_t num_iovs);

int
niova_io_fd_nonblocking(int fd);

ssize_t
niova_io_iovs_advance(struct iovec *iovs, size_t niovs,
                      off_t bytes_already_consumed,
                      struct iovec *save_iov);

int
niova_io_iov_restore(struct iovec *iovs, size_t niovs, size_t save_idx,
                     const struct iovec *save_iov);

#endif
