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

    return read_rc;
}
