/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

ssize_t
io_read(int fd, char *buf, size_t size)
{
    if (fd < 0)
        return -EBADF;

    else if (!buf)
        return -EINVAL;

    size_t bytes_read = 0;
    ssize_t read_rc = 0;

    for (; bytes_read < size; bytes_read += read_rc)
    {
        read_rc = read(fd, buf + bytes_read, size - bytes_read);

        if (read_rc < 0 && (errno != EINTR))
            return -errno;

        else if (!read_rc)
            break;
    }

    return bytes_read;
}

ssize_t
io_pwrite(int fd, const char *buf, size_t size, off_t offset)
{
    if (fd < 0)
        return -EBADF;

    else if (!buf)
        return -EINVAL;

    size_t bytes_written = 0;
    ssize_t pwrite_rc = 0;

    for (; bytes_written < size; bytes_written += pwrite_rc)
    {
        pwrite_rc = pwrite(fd, buf + bytes_written, size - bytes_written,
                           offset + bytes_written);

        if (pwrite_rc < 0 && (errno != EINTR))
            return -errno;
    }

    return bytes_written;
}

ssize_t
io_pread(int fd, char *buf, size_t size, off_t offset)
{
    if (fd < 0)
        return -EBADF;

    else if (!buf)
        return -EINVAL;

    size_t bytes_read = 0;
    ssize_t pread_rc = 0;

    for (; bytes_read < size; bytes_read += pread_rc)
    {
        pread_rc = pread(fd, buf + bytes_read, size - bytes_read,
                         offset + bytes_read);

        if (pread_rc < 0 && (errno != EINTR))
            return -errno;

        else if (!pread_rc)
            break;
    }

    return bytes_read;
}

int
io_fsync(int fd)
{
    return fsync(fd);
}
