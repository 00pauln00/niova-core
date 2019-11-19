/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

/* -- ev_pipe --
 * Attempts to provide an efficient blocking interface which avoids
 * system calls when possible.  To do this, the library uses two counters
 * which are consulted to determine whether a write() to the pipe is needed,
 * thus preventing the need for one write per event completion.
 */

#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
#undef _GNU_SOURCE

#include "common.h"
#include "ev_pipe.h"
#include "log.h"

#define PIPE_DRAIN_SIZE 256

/**
 * ev_pipe_notify - write data into the event pipe.  This must always be
 *   called by the same thead to avoid lost increments to evp_writer_cnt.
 */
ev_pipe_writer_t
ev_pipe_notify(struct ev_pipe *evp)
{
    if (!evp)
        return;

    int rc = 0;

    while (evp->evp_writer_cnt < evp->evp_reader_cnt)
    {
        char c[EV_PIPE_WRITE_SZ] = {'x'};

        rc = write(evp->evp_pipe[WRITE_PIPE_IDX], c, EV_PIPE_WRITE_SZ);

        if (rc == EV_PIPE_WRITE_SZ)
        {
            uint64_t old_val = evp->evp_writer_cnt;
            evp->evp_writer_cnt++;
            rc = 0;

            SIMPLE_LOG_MSG(LL_DEBUG, "%ld old=%ld", evp->evp_writer_cnt,
                           old_val);
        }
        else
        {
            rc = -errno;
            break;
        }
    }

    if (rc)
        SIMPLE_LOG_MSG(LL_WARN, "%s", strerror(-rc));
}

/**
 * ev_pipe_cleanup - close the pipe fd's.
 */
int
ev_pipe_cleanup(struct ev_pipe *evp)
{
    if (!evp)
        return -EINVAL;

    int save_errno[NUM_PIPE_FD];

    for (int i = 0; i < NUM_PIPE_FD; i++)
        save_errno[i] = close(evp->evp_pipe[i]) ? errno : 0;

    const enum log_level level = (save_errno[0] || save_errno[1]) ?
        LL_WARN : LL_TRACE;

    SIMPLE_LOG_MSG(level, "%s", strerror((save_errno[0] || save_errno[1])));

    return save_errno[0] ? save_errno[0] : save_errno[1];
}

ssize_t
ev_pipe_drain(struct ev_pipe *evp)
{
    char sink_buf[PIPE_DRAIN_SIZE];
    ssize_t num_bytes = 0, rc = 0;

    for (;;)
    {
        rc = read(evp_read_fd_get(evp), sink_buf, PIPE_DRAIN_SIZE);

        if (rc > 0)
            num_bytes += rc;

        if (!rc || rc < 0 || rc < PIPE_DRAIN_SIZE)
            break;
    }

    if (rc < 0)
        num_bytes = -errno;

    return num_bytes;
}

/**
 * ev_pipe_setup - prepares the ev_pipe for usage.
 */
int
ev_pipe_setup(struct ev_pipe *evp)
{
    if (!evp)
        return -EINVAL;

    int rc = pipe(evp->evp_pipe);

    for (int i = 0; i < NUM_PIPE_FD && !rc; i++)
    {
        rc = fcntl(evp->evp_pipe[i], F_SETPIPE_SZ, EV_PIPE_SZ);

        if (rc == EV_PIPE_SZ)
            rc = fcntl(evp->evp_pipe[i], F_SETFL, O_NONBLOCK);
    }

    rc = rc ? -errno : 0;

    if (rc)
        (int)ev_pipe_cleanup(evp);

    SIMPLE_LOG_MSG(LL_NOTIFY, "%s", strerror(-rc));

    return rc;
}

/**
 * evp_read_fd_get - returns the read fd from the event_pipe.  This fd may be
 *   held by the caller and possibly incorporated into an epoll or poll fd set.
 */
int
evp_read_fd_get(const struct ev_pipe *evp)
{
    if (!evp)
        return -EINVAL;

    return evp->evp_pipe[READ_PIPE_IDX];
}
