/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "common.h"
#include "io.h"
#include "log.h"

ssize_t
niova_io_read(int fd, char *buf, size_t size)
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
niova_io_pwrite(int fd, const char *buf, size_t size, off_t offset)
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
niova_io_pread(int fd, char *buf, size_t size, off_t offset)
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
niova_io_ftruncate(int fd, off_t length)
{
    int rc = ftruncate(fd, length);

    return rc ? -errno : 0;
}

int
niova_io_fsync(int fd)
{
    int rc = fsync(fd);

    return rc ? -errno : 0;
}

/**
 * niova_io_fd_drain - helper function for non-blocking FDs which empties the
 *    contents from the file descriptor.  This can be used in epoll callbacks
 *    to empty pending data from the fd.  It's expected that this function
 *    returns 0, signifying that all data have been read and the fd has been
 *    emptied.
 * @fd:  file descriptor to be emptied.
 * @ret_data:  an optional parameter to store the contents of the file
 *    descriptor in an aggregated count.
 */
ssize_t
niova_io_fd_drain(int fd, size_t *ret_data)
{
    ssize_t rrc, val, total;

    for (rrc = 1, total = 0; rrc > 0; total += val)
    {
        rrc = read(fd, &val, sizeof(ssize_t));
        if (rrc < 0 && errno != EINTR)
        {
            rrc = -errno;
            break;
        }
    }

    if (ret_data)
        *ret_data = total;

    return (rrc == -EAGAIN || rrc == -EWOULDBLOCK) ? 0 : rrc;
}

/**
 * niova_io_iovs_map_consumed - given a set of source iov's, map them to the
 *   set of destination iov's based on the number of bytes which have already
 *   been processed.
 * @src:  array of input iov's
 * @dest:  array of output iov's which should be the same size as the 'src'
 *    array.
 * @num_iovs:  number of iov's in both 'src' and 'dest'.
 * @bytes_already_consumed:  the total number of bytes from the 'src' iov
 *    array which have been processed.
 * Returns:  a positive number <= num_iovs which represents the number of
 *    iov's which map unconsumed data.
 */
ssize_t
niova_io_iovs_map_consumed(const struct iovec *src, struct iovec *dest,
                           const size_t num_iovs,
                           size_t bytes_already_consumed, ssize_t max_bytes)
{
    if (!src || !dest || !num_iovs || src == dest ||
        (dest > src && dest < &src[num_iovs]) ||
        (src > dest && src < &dest[num_iovs]))
        return -EINVAL;

    ssize_t dest_num_iovs = 0;
    ssize_t bytes_remaining = max_bytes;

    for (size_t i = 0; i < num_iovs; i++)
    {
        if (bytes_already_consumed < src[i].iov_len)
        {
            const size_t idx = dest_num_iovs++;

            dest[idx].iov_len = src[i].iov_len - bytes_already_consumed;
            dest[idx].iov_base =
                (char *)src[i].iov_base + bytes_already_consumed;

            bytes_remaining -= dest[idx].iov_len;

            // Handle the 'max_bytes' option, reducing the iov_len if needed
            if (max_bytes > 0 && bytes_remaining <= 0)
            {
                NIOVA_ASSERT(ABS(bytes_remaining) <= dest[idx].iov_len);
                dest[idx].iov_len -= ABS(bytes_remaining);
                break;
            }
        }

        bytes_already_consumed -= MIN(bytes_already_consumed, src[i].iov_len);
    }

    // User requested max does not fit into the provided iovs
    if (max_bytes > 0 && bytes_remaining > 0)
        return -EOVERFLOW;

    return dest_num_iovs;
}

ssize_t
niova_io_iovs_map_consumed2(const struct iovec *src, struct iovec *dest,
                            const size_t num_src_iovs,
                            const size_t num_dest_iovs,
                            size_t bytes_already_consumed, ssize_t max_bytes)
{
    if (!src || !dest || !num_src_iovs || !num_dest_iovs || src == dest ||
        (dest > src && dest < &src[num_src_iovs]) ||
        (src > dest && src < &dest[num_dest_iovs]))
        return -EINVAL;

    if (max_bytes <= 0 && num_src_iovs > num_dest_iovs)
        return -EOVERFLOW;

    ssize_t xdest_niovs = 0;
    ssize_t bytes_remaining = max_bytes;

    for (size_t i = 0; i < num_src_iovs; i++)
    {
        if (bytes_already_consumed < src[i].iov_len)
        {
            const size_t idx = xdest_niovs++;

            if (idx >= num_dest_iovs)
                break;

            dest[idx].iov_len = src[i].iov_len - bytes_already_consumed;
            dest[idx].iov_base =
                (char *)src[i].iov_base + bytes_already_consumed;

            bytes_remaining -= dest[idx].iov_len;

            // Handle the 'max_bytes' option, reducing the iov_len if needed
            if (max_bytes > 0 && bytes_remaining <= 0)
            {
                NIOVA_ASSERT(ABS(bytes_remaining) <= dest[idx].iov_len);
                dest[idx].iov_len -= ABS(bytes_remaining);
                break;
            }
        }

        bytes_already_consumed -= MIN(bytes_already_consumed, src[i].iov_len);
    }

    // User requested max does not fit into the provided iovs
    if (max_bytes > 0 && bytes_remaining > 0)
        return -EOVERFLOW;

    return xdest_niovs;
}

ssize_t
niova_io_copy_to_iovs(const char *src, size_t src_size,
                      struct iovec *dest_iovs, const size_t num_iovs)
{
    if (!src || !src_size || !dest_iovs || !num_iovs)
        return -EINVAL;

    size_t bytes_copied = 0;

    for (size_t i = 0; ((bytes_copied < src_size) && (i < num_iovs)); i++)
    {
        size_t n = MIN(dest_iovs[i].iov_len, (src_size - bytes_copied));
        memcpy(dest_iovs[i].iov_base, &src[bytes_copied], n);
        bytes_copied += n;
    }

    return bytes_copied;
}

ssize_t
niova_io_memset_iovs(struct iovec *iovs, size_t num_iovs, int c, size_t len)
{
    if (!len || !iovs || !num_iovs)
        return -EINVAL;

    size_t remaining = len;

    for (size_t i = 0; i < num_iovs && remaining; i++)
    {
        size_t x = MIN(remaining, iovs[i].iov_len);
        memset(iovs[i].iov_base, c, x);

        remaining -= x;
    }

    return len - remaining;
}

ssize_t
niova_io_copy_from_iovs(char *dest, const size_t dest_size,
                        const struct iovec *src_iovs, const size_t num_iovs)
{
    if (!dest || !dest_size || !src_iovs || !num_iovs)
        return -EINVAL;

    size_t cnt = 0;

    for (size_t i = 0; i < num_iovs && cnt < dest_size; i++)
    {
        size_t len = MIN(src_iovs[i].iov_len, (dest_size - cnt));

        memcpy(&dest[cnt], src_iovs[i].iov_base, len);
        cnt += len;
    }

    return cnt;
}

int
niova_io_fd_nonblocking(int fd)
{
    if (fd < 0)
        return -EINVAL;

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -errno;

    return (flags & O_NONBLOCK) ? fcntl(fd, F_SETFL, (flags & O_NONBLOCK)) : 0;
}

/**
 * niova_io_iovs_advance - an iov continuation method which can be non-destructive
 *    while only requiring a single iov for restoring state.  The function will
 *    fast-forward to the current iov, based on the 'bytes_already_consumed'
 *    parameter, and return the index of the iov to be used in the next io
 *    request.  If 'bytes_already_consumed' points into an iov, that iov is
 *    modified to reflect the already completed work.  Prior to modifying the
 *    iov, it may be optionally saved into the 'save_iov' parameter.
 * @iovs:  array iovs
 * @niovs:  number of iovs to be considered for processing
 * @bytes_already_consumed:  number of bytes in the iov set which have been
 *    processed prior to calling this function.
 * @save_iov:  temp iov which can be used for restoring the iov array to its
 *    original state.
 */
ssize_t
niova_io_iovs_advance(struct iovec *iovs, size_t niovs,
                      off_t bytes_already_consumed, struct iovec *save_iov)
{
    if (!iovs || !niovs || bytes_already_consumed < 0)
        return -EINVAL;

    if (save_iov >= &iovs[0] && save_iov <= &iovs[niovs - 1])
        return -EFAULT;

    if (bytes_already_consumed == 0) // noop
    {
        if (save_iov)
        {
            save_iov->iov_base = iovs[0].iov_base;
            save_iov->iov_len = iovs[0].iov_len;
        }
        return 0;
    }

    size_t idx;
    for (idx = 0; idx < niovs && bytes_already_consumed; idx++)
    {
        bytes_already_consumed -= iovs[idx].iov_len;
        if (bytes_already_consumed < 0) // don't increment idx
            break;
    }

    if (idx == niovs)
    {
        if (bytes_already_consumed > 0)
        {
            NIOVA_ASSERT(idx == niovs);
            return -ERANGE;
        }
        else
        {
            /* If this advance consumes all iov space, simply return w/out
             * modifying any iov.
             */
            NIOVA_ASSERT(bytes_already_consumed == 0);
            return -EXFULL;
        }
    }

    NIOVA_ASSERT(idx < niovs);

    if (save_iov)
    {
        save_iov->iov_base = iovs[idx].iov_base;
        save_iov->iov_len = iovs[idx].iov_len;
    }

    if (bytes_already_consumed < 0)  // Modify idx iov
    {
        char *base = iovs[idx].iov_base;
        base += (iovs[idx].iov_len + bytes_already_consumed);

        iovs[idx].iov_len = ABS(bytes_already_consumed);
        iovs[idx].iov_base = (void *)base;
    }

    return idx;
}

/**
 * niova_io_iov_restore - used in conjunction with niova_io_iovs_advance() to
 *    replace a modified iov member with its original contents.
 * @iovs:  iov array
 * @niovs:  size of the array
 * @save_idx:  index value to restore
 * @save_iov:  iov contents to be restored
 */
int
niova_io_iov_restore(struct iovec *iovs, size_t niovs, size_t save_idx,
                     const struct iovec *save_iov)
{
    if (!iovs || !niovs || save_idx >= niovs || !save_iov)
        return -EINVAL;

    iovs[save_idx] = *save_iov;

    return 0;
}

/**
 * iovec iterator - calls callback once per iovec.
 *
 * @iovs:      Array of iovecs containing the data
 * @niovs:     Number of iovecs in the array
 * @num_bytes: Number of bytes to iterate over
 * @cb:        Callback function invoked for each iovec segment
 * @user_ctx:  Context pointer passed through to callback
 *
 */
ssize_t
niova_io_iterate_iovs(const struct iovec *iovs, size_t niovs,
                      size_t num_bytes, niova_iov_cb_t cb, void *user_ctx)
{
    if (!iovs || !niovs || !cb)
        return -EINVAL;

    if (num_bytes == 0)
        return 0;

    size_t bytes_remaining = num_bytes;

    for (size_t iov_idx = 0; bytes_remaining > 0 && iov_idx < niovs; iov_idx++)
    {
        size_t segment_len = MIN(iovs[iov_idx].iov_len, bytes_remaining);
        int cb_rc = cb(iovs[iov_idx].iov_base, segment_len, user_ctx);
        if (cb_rc != 0)
            return cb_rc;

        bytes_remaining -= segment_len;
    }

    if (bytes_remaining > 0)
        return -ERANGE;

    return (ssize_t)(num_bytes - bytes_remaining);
}