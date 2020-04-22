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

#include "log.h"
#include "io.h"

/**
 * file_util_open_and_read - utility function which reads a file into the
 *    provided buffer.  This is meant for use by relatively simplistic
 *    operations such as reading config files.  The function will return
 *    -E2BIG if the provided buffer is smaller than the file.
 * @dirfd:  FD to the directory where the file resides.  This can be -1 so long
 *    as the file_name is for an absolute path.  Specifying AT_FDCWD will use
 *    the process's CWD.
 * @file_name:  The relative or absolute path name of the file.
 * @output_buf:  Buffer where the contents should be placed.
 * @output_size:  Size of the buffer.
 * @ret_fd:  If specified, the function will copy the file's FD and leave the
 *    file open.  It's the caller's responsibility to call close().
 */
ssize_t
file_util_open_and_read(int dirfd, const char *file_name, char *output_buf,
                        size_t output_size, int *ret_fd)
{
    if (!file_name || !output_buf || !output_size ||
        !(dirfd >= 0 || dirfd == AT_FDCWD || output_buf[0] == '/'))
        return -EINVAL;

    struct stat stb;
    bool proc_file = false;

    if (!strncmp("/proc/", file_name, 6))
        proc_file = true;

    /* Lookup the file, check the type and file size.
     */
    int rc = fstatat(dirfd, file_name, &stb, AT_SYMLINK_NOFOLLOW);
    if (rc < 0)
	return -errno;

    else if (!S_ISREG(stb.st_mode))
        return -ENOTSUP;

    else if (!proc_file && stb.st_size > output_size)
	return -E2BIG;

    int fd = openat(dirfd, file_name, O_RDONLY);
    if (fd < 0)
        return -errno;

    bool close_fd = ret_fd ? false : true;

    ssize_t io_rc = io_read(fd, output_buf,
                            proc_file ? output_size : stb.st_size);
    if (io_rc < 0)
    {
        io_rc = -errno;
        close_fd = true;
    }
    else if ((!proc_file && io_rc != stb.st_size) ||
             (proc_file && io_rc == output_size))
    {
        io_rc = -EMSGSIZE;
        close_fd = true;
    }
    else if (ret_fd)
    {
        *ret_fd = fd;
    }

    if (close_fd)
    {
        int close_rc = close(fd);
        if (close_rc)
            SIMPLE_LOG_MSG(LL_WARN, "close(`%s'): %s", file_name,
                           strerror(errno));
    }

    return io_rc;
}
