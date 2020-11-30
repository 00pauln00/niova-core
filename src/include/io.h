/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _NIOVA_IO_H_
#define _NIOVA_IO_H_ 1

#include <sys/socket.h>

#define IO_MAX_IOVS 256

ssize_t
io_read(int fd, char *buf, size_t size);

ssize_t
io_pwrite(int fd, const char *buf, size_t size, off_t offset);

ssize_t
io_pread(int fd, char *buf, size_t size, off_t offset);

int
io_ftruncate(int fd, off_t length);

int
io_fsync(int fd);

ssize_t
io_fd_drain(int fd, size_t *ret_data);

static inline size_t
io_iovs_total_size_get(const struct iovec *iovs, const size_t iovlen)
{
    size_t total_size = 0;

    if (iovs)
        for (size_t i = 0; i < iovlen; i++)
            total_size += iovs[i].iov_len;

    return total_size;
}

ssize_t
io_iovs_map_consumed(const struct iovec *src, struct iovec *dest,
                     const size_t num_iovs, size_t bytes_already_consumed);

ssize_t
io_copy_to_iovs(const char *src, size_t src_size, struct iovec *dest_iovs,
                const size_t num_iovs);

int
io_fd_nonblocking(int fd);

#endif
