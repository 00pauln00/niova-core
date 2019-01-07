/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef NIOSD_IO_H
#define NIOSD_IO_H 1

#define _GNU_SOURCE 1 // for O_DIRECT
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>

#include "common.h"
#include "atomic.h"

#define NIOSD_MAX_AIO_EVENTS       65536
#define NIOSD_MAX_AIO_NREQS_SUBMIT 256

#define NIOSD_GETEVENTS_MIN        1
#define NIOSD_GETEVENTS_MAX        NIOSD_MAX_AIO_NREQS_SUBMIT

/* Submitter thread context
 */
typedef void     niosd_io_submitter_ctx_t;
typedef int      niosd_io_submitter_ctx_int_t;
typedef int64_t  niosd_io_submitter_ctx_int64_t;
typedef uint64_t niosd_io_submitter_ctx_uint64_t;

/* Event thread context
 */
typedef void     niosd_io_event_ctx_t;
typedef bool     niosd_io_event_ctx_bool_t;
typedef int      niosd_io_event_ctx_int_t;
typedef int64_t  niosd_io_event_ctx_int64_t;
typedef uint64_t niosd_io_event_ctx_uint64_t;

/* Completion thread context
 */
typedef void     niosd_io_completion_cb_ctx_t;
typedef int      niosd_io_completion_cb_ctx_int_t;
typedef int64_t  niosd_io_completion_cb_ctx_int64_t;
typedef uint64_t niosd_io_completion_cb_ctx_uint64_t;
typedef size_t   niosd_io_completion_cb_ctx_size_t;

enum niosd_dev_status
{
    NIOSD_DEV_STATUS_INVALID = 0,
    NIOSD_DEV_STATUS_STOPPED,
    NIOSD_DEV_STATUS_RUNNING,
    NIOSD_DEV_STATUS_STARTING,
    NIOSD_DEV_STATUS_ERROR,
};

enum niosd_io_request_type
{
    NIOSD_REQ_TYPE_UNINIT  = 0,
    NIOSD_REQ_TYPE_NOOP    = 1,
    NIOSD_REQ_TYPE_PREAD   = 2,
    NIOSD_REQ_TYPE_PWRITE  = 3,
    NIOSD_REQ_TYPE_FSYNC   = 4,
    NIOSD_REQ_TYPE_DISCARD = 5,
//  NIOSD_REQ_TYPE_PREADV  = 6,
//  NIOSD_REQ_TYPE_PWRITEV = 7,
};

#define NIOSD_IO_CTXS_MAX 2

enum niosd_io_ctx_type
{
    NIOSD_IO_CTX_TYPE_DEFAULT   = 0,
    NIOSD_IO_CTX_TYPE_COMPACTOR = 1,
    NIOSD_IO_CTX_TYPE_MAX       = 2,
};

struct niosd_io_compl_event_ring
{
    niosd_io_submitter_ctx_uint64_t     CACHE_ALIGN_MEMBER(niocer_nsub);
    niosd_io_event_ctx_uint64_t         CACHE_ALIGN_MEMBER(niocer_head);
    niosd_io_completion_cb_ctx_uint64_t CACHE_ALIGN_MEMBER(niocer_tail);
    struct io_event                    *niocer_events;
    const size_t                        niocer_num_events;
};

#define niosd_ctx_increment_cer_counter(nioctx, counter, value) \
    (nioctx)->nioctx_cer.niocer_##counter += value

#define niosd_ctx_decrement_cer_counter(nioctx, counter, value) \
    (nioctx)->nioctx_cer.niocer_##counter -= value

#define niosd_ctx_to_cer_counter(nioctx, counter)       \
    (nioctx)->nioctx_cer.niocer_##counter

#define niosd_ctx_to_cer_memb niosd_ctx_to_cer_counter

#define niosd_ctx_to_cer_event(nioctx, event_slot)      \
    &(nioctx)->nioctx_cer.niocer_events[event_slot]

struct niosd_io_ctx
{
    bool                             nioctx_ready;
    enum niosd_io_ctx_type           nioctx_type;
    io_context_t                     nioctx_ctx;
    pthread_t                        nioctx_event_thread;
    struct niosd_io_compl_event_ring nioctx_cer;
};

struct niosd_device
{
    char                             ndev_name[PATH_MAX + 1];
    struct stat                      ndev_stb;
    int                              ndev_fd;
    enum niosd_dev_status            ndev_status;
    struct niosd_io_ctx              ndev_ctxs[NIOSD_IO_CTX_TYPE_MAX];
};


static inline ssize_t
niosd_ctx_pending_io_ops(const struct niosd_io_ctx *nioctx)
{
    ssize_t pending_io_ops =
        niosd_ctx_to_cer_counter(nioctx, nsub) -
        niosd_ctx_to_cer_counter(nioctx, head);

    NIOVA_ASSERT(pending_io_ops >= 0);

    return pending_io_ops;
}

static inline ssize_t
niosd_ctx_pending_completion_ops(const struct niosd_io_ctx *nioctx)
{
    ssize_t pending_completion_ops =
        niosd_ctx_to_cer_counter(nioctx, head) -
        niosd_ctx_to_cer_counter(nioctx, tail);

    NIOVA_ASSERT(pending_completion_ops >= 0);

    return pending_completion_ops;
}

static inline ssize_t
niosd_ctx_total_pending_ops(const struct niosd_io_ctx *nioctx)
{
    return niosd_ctx_pending_io_ops(nioctx) +
        niosd_ctx_pending_completion_ops(nioctx);
}

static inline int
niosd_device_to_fd(const struct niosd_device *ndev)
{
    return ndev->ndev_fd;
}

static inline size_t
niosd_nsectors_to_bytes(uint32_t nsectors)
{
    return (size_t)(nsectors * NIOVA_SECTOR_SIZE);
}

static inline long long
niosd_pblk_to_offset(pblk_id_t pblk_id)
{
    return (long long)(pblk_id * PBLK_SIZE_BYTES);
}

struct niosd_io_request;

typedef void (*niosd_io_callback_t)(struct niosd_io_request *);

/**
 * -- niosd_io_request --
 * The request structure which holds all relevant information for request
 * submission and completion.  Currently, this structure embeds linux aio
 * members.
 * @niorq_dev:  The device on which this request will run.
 * @niorq_ctx:  Context state used by linux AIO.
 * @niorq_iocb:  Linux AIO IOCB request submission structure.
 * @niorq_pblk_id:  Physical block ID on which to operate.
 * @niorq_nsectors:  Number of 512-byte sectors used in the request.
 * @niorq_type:  Type of the request.
 * @niorq_sink_buf:  Buffer pointer to which reads are copied.
 * @niorq_src_buf:  Buffer pointer from which writes are copied.
 * @niorq_completion_event:  Completion event information (linux AIO).
 *    Operation return codes are held here.
 * @niorq_cb:  Request callback.
 * @niorq_cb_data:  Request callback data.
 */
struct niosd_io_request
{
    struct niosd_io_ctx       *niorq_ctx;
    struct iocb                niorq_iocb;
    pblk_id_t                  niorq_pblk_id;
    uint32_t                   niorq_nsectors;
    enum niosd_io_request_type niorq_type;
    union
    {
        void                  *niorq_sink_buf;
        const void            *niorq_src_buf;
    };
    niosd_io_callback_t        niorq_cb;
    void                      *niorq_cb_data;
};

#define DBG_NIOSD_REQ(log_level, iorq, fmt, ...)                        \
    log_msg(log_level,                                                  \
            "iorq@%p %s(%zu:%zu) pblk:%x ns:%u t:%d buf:%p "fmt,      \
            (iorq), niosd_ctx_to_device((iorq)->niorq_ctx)->ndev_name,  \
            niosd_ctx_pending_io_ops((iorq)->niorq_ctx),                \
            niosd_ctx_pending_completion_ops((iorq)->niorq_ctx),        \
            (iorq)->niorq_pblk_id, (iorq)->niorq_nsectors,              \
            (iorq)->niorq_type, (iorq)->niorq_src_buf, ##__VA_ARGS__)

static inline struct niosd_io_ctx *
niosd_device_to_ctx(struct niosd_device *ndev, enum niosd_io_ctx_type type)
{
    NIOVA_ASSERT(type < NIOSD_IO_CTX_TYPE_MAX);

    return &ndev->ndev_ctxs[type];
}

static inline struct niosd_device *
niosd_ctx_to_device(struct niosd_io_ctx *nioctx)
{
    NIOVA_ASSERT(nioctx->nioctx_type < NIOSD_IO_CTX_TYPE_MAX);

    return (struct niosd_device *)((char *)nioctx -
                                   offsetof(struct niosd_device,
                                            ndev_ctxs[nioctx->nioctx_type]));
}

niosd_io_submitter_ctx_t
niosd_device_params_init(const char *, struct niosd_device *);

niosd_io_submitter_ctx_int_t
niosd_device_open(struct niosd_device *);

niosd_io_submitter_ctx_int_t
niosd_device_close(struct niosd_device *);

niosd_io_submitter_ctx_int_t
niosd_io_request_init(struct niosd_io_request *, struct niosd_io_ctx *,
                      pblk_id_t, enum niosd_io_request_type, uint32_t, void *,
                      niosd_io_callback_t, void *);

niosd_io_submitter_ctx_int_t
niosd_io_submit(struct niosd_io_request **, long int);

niosd_io_completion_cb_ctx_size_t
niosd_io_events_complete(struct niosd_io_ctx *, long int);

#endif
