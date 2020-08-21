/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _EV_PIPE_
#define _EV_PIPE_ 1

#include "atomic.h"
#include "common.h"

#define EV_PIPE_WRITE_SZ 1
#define EV_PIPE_SZ       4096

typedef void     ev_pipe_reader_t;
typedef void     ev_pipe_writer_t;

typedef int      ev_pipe_reader_int_t;
typedef int      ev_pipe_writer_int_t;

typedef uint64_t         ev_pipe_reader_uint64_t;
typedef niova_atomic64_t ev_pipe_writer_uint64_t;

struct ev_pipe
{
    int                     evp_pipe[NUM_PIPE_FD];
    ev_pipe_reader_uint64_t CACHE_ALIGN_MEMBER(evp_reader_cnt);
    ev_pipe_writer_uint64_t CACHE_ALIGN_MEMBER(evp_writer_cnt);
};

ev_pipe_writer_t
ev_pipe_notify(struct ev_pipe *evp);

int
ev_pipe_cleanup(struct ev_pipe *evp);

int
ev_pipe_setup(struct ev_pipe *evp);

ev_pipe_reader_int_t
evp_read_fd_get(const struct ev_pipe *evp);

void
ev_pipe_reset(struct ev_pipe *evp, const char *function, const int lineno);

#define EV_PIPE_RESET(evp)                      \
    ev_pipe_reset(evp, __func__, __LINE__);

#endif
