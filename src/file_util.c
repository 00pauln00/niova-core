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
#include <limits.h>

#include "common.h"
#include "io.h"
#include "log.h"
#include "registry.h"

REGISTRY_ENTRY_FILE_GENERATE;

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

    else if (!proc_file && (size_t)stb.st_size > output_size)
        return -E2BIG;

    else if (!proc_file && !stb.st_size) // nothing to read
        return -ENODATA;

    int fd = openat(dirfd, file_name, O_RDONLY);
    if (fd < 0)
        return -errno;

    bool close_fd = ret_fd ? false : true;

    ssize_t io_rc =
        niova_io_read(fd, output_buf,
                      proc_file ? output_size : (size_t)stb.st_size);
    if (io_rc < 0)
    {
        io_rc = -errno;
        close_fd = true;
    }
    else if (!io_rc)
    {
        io_rc = -ENODATA;
        close_fd = true;
    }
    else if ((!proc_file && io_rc != stb.st_size) ||
             (proc_file && io_rc == (ssize_t)output_size))
    {
        io_rc = -EMSGSIZE;
        close_fd = true;
    }
    else if (ret_fd)
    {
        *ret_fd = fd;
    }

    if (io_rc > 0 && io_rc < (ssize_t)output_size)
        output_buf[io_rc] = '\0';  // terminate the output buffer

    if (close_fd)
    {
        int close_rc = close(fd);
        if (close_rc)
            SIMPLE_LOG_MSG(LL_WARN, "close(`%s'): %s", file_name,
                           strerror(errno));
    }

    return io_rc;
}

static int
file_util_try_mkdir(const char *path)
{
    if (!path)
        return -EINVAL;

    else if (strnlen(path, PATH_MAX) >= PATH_MAX)
        return -ENAMETOOLONG;

    LOG_MSG(LL_DEBUG, "%s", path);

    struct stat stb;
    int rc = stat(path, &stb);
    if (!rc)
        return S_ISDIR(stb.st_mode) ? 0 : -ENOTDIR;

    rc = -errno;
    if (rc != -ENOENT) // Only continue if the path did not exist
        return rc;

    rc = mkdir(path, 0755);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "mkdir('%s'): %s", path, strerror(-rc));

        rc = -errno;
        if (rc == -EEXIST)
        {
            int stat_rc = stat(path, &stb);
            if (!stat_rc && S_ISDIR(stb.st_mode))
                rc = 0;
        }
    }

    return rc;
}

int
file_util_pathname_build(const char *path)
{
    if (!path)
        return -EINVAL;

    const size_t path_len = strnlen(path, PATH_MAX);

    if (path_len >= PATH_MAX)
        return -ENAMETOOLONG;

    int rc = file_util_try_mkdir(path);
    if (!rc || rc != -ENOENT) // only continue if the path did not exist
        return rc;

    char sub_path[path_len];
    strncpy(sub_path, path, path_len);

    size_t chopped_path_cnt = 0;
    bool found_delimiter = false;
    for (ssize_t i = path_len - 1; i >= 0; i--)
    {
        if (sub_path[i] == '/')
        {
            sub_path[i] = '\0';
            found_delimiter = true;
        }
        else
        {
            chopped_path_cnt++;

            if (found_delimiter)
            {
                file_util_pathname_build(sub_path);
                break;
            }
        }
    }

    // Only retry the mkdir if a prior path component was found.
    return (chopped_path_cnt && found_delimiter) ?
        file_util_try_mkdir(path) : -EINVAL;
}
