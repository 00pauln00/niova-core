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
#include <time.h>

#include "common.h"
#include "binary_hist.h"
#include "ctor.h"
#include "ev_pipe.h"
#include "registry.h"

#define NIOSD_MAX_AIO_EVENTS       1048576
#define NIOSD_DEFAULT_AIO_EVENTS   65536
#define NIOSD_LOWER_AIO_EVENTS     8192
#define NIOSD_MIN_AIO_EVENTS       128

#define NIOSD_MAX_AIO_NREQS_SUBMIT 256

#define NIOSD_GETEVENTS_MIN        1
#define NIOSD_GETEVENTS_MAX        NIOSD_MAX_AIO_NREQS_SUBMIT

/* Submitter thread context
 */
typedef void     niosd_io_submitter_ctx_t;
typedef int      niosd_io_submitter_ctx_int_t;
typedef int64_t  niosd_io_submitter_ctx_int64_t;
typedef uint64_t niosd_io_submitter_ctx_uint64_t;

/* Blocking on event completion (not io_getevents), this should be the same
 * thread context as the submitter.
 */
typedef void     niosd_io_blocking_ctx_t;
typedef uint64_t niosd_io_blocking_ctx_uint64_t;

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
    NIOSD_DEV_STATUS_STARTUP_DEV_OPEN, // device is opening
    NIOSD_DEV_STATUS_STARTUP_SB_SCAN, // superblocks are being scanned
    NIOSD_DEV_STATUS_STARTUP_CHAIN_SCAN, // chunk chains are being scanned
    NIOSD_DEV_STATUS_RUNNING,
    NIOSD_DEV_STATUS_STOPPED,
    NIOSD_DEV_STATUS_ERROR,
    NIOSD_DEV_STATUS_SB_INIT, // initialize the superblock with a new uuid
    NIOSD_DEV_STATUS_SB_ERROR // error was observed while reading SB chain
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
    NIOSD_IO_CTX_TYPE_ANY       = 0,
    NIOSD_IO_CTX_TYPE_DEFAULT   = 1,
    NIOSD_IO_CTX_TYPE_COMPACTOR = 2,
    NIOSD_IO_CTX_TYPE_MAX       = 3,
};

static inline char
niosd_io_ctx_type_to_char(const enum niosd_io_ctx_type t)
{
    switch (t)
    {
    case NIOSD_IO_CTX_TYPE_DEFAULT:
        return 'D';
    case NIOSD_IO_CTX_TYPE_COMPACTOR:
        return 'C';
    default:
        break;
    }

    return 'U';
}

static inline const char *
niosd_io_ctx_type_to_string(const enum niosd_io_ctx_type t)
{
    switch (t)
    {
    case NIOSD_IO_CTX_TYPE_DEFAULT:
        return "user-io-ctx";
    case NIOSD_IO_CTX_TYPE_COMPACTOR:
        return "system-io-ctx";
    default:
        break;
    }

    return "unknown";
}

struct niosd_io_compl_event_ring
{
    niosd_io_submitter_ctx_uint64_t     CACHE_ALIGN_MEMBER(niocer_nsub);
    niosd_io_event_ctx_uint64_t         CACHE_ALIGN_MEMBER(niocer_head);
    niosd_io_completion_cb_ctx_uint64_t CACHE_ALIGN_MEMBER(niocer_tail);
    struct io_event                    *niocer_events;
    const uint64_t                      niocer_num_events;
    const uint64_t                      niocer_event_mask;
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


enum niosd_io_ctx_stats_hist
{
    NICSH_RD_SIZE_IN_BYTES   = 0,
    NICSH_WR_SIZE_IN_BYTES   = 1,
    NICSH_RD_LATENCY_USEC    = 2,
    NICSH_WR_LATENCY_USEC    = 3,
    NICSH_IO_TO_CB_TIME_USEC = 4,
    NICSH_IO_NUM_PENDING     = 5,
    NICSH_IO_CTX_STATS_MAX   = 6,
};

static inline const char *
niosd_io_ctx_stats_hist_2_name(enum niosd_io_ctx_stats_hist stat)
{
    switch (stat)
    {
    case NICSH_RD_SIZE_IN_BYTES:
        return "read_size_in_bytes";
    case NICSH_WR_SIZE_IN_BYTES:
        return "write_size_in_bytes";
    case NICSH_RD_LATENCY_USEC:
        return "read_latency_usec";
    case NICSH_WR_LATENCY_USEC:
        return "write_latency_usec";
    case NICSH_IO_TO_CB_TIME_USEC:
        return "cb_queue_latency";
    case NICSH_IO_NUM_PENDING:
        return "io_pending_count";
    default:
        return "unknown";
    }
}

#define NICSH_DEF_IO_SIZE_START_BIT 9
#define NICSH_DEF_IO_SIZE_NBUCKETS  9

#define NICSH_DEF_IO_LAT_START_BIT  2
#define NICSH_DEF_IO_LAT_NBUCKETS   19

#define NICSH_DEF_IO_TO_CB_START_BIT 2
#define NICSH_DEF_IO_TO_CB_LAT_NBUCKETS 19

#define NICSH_DEF_IO_NUM_PDNG_START_BIT 0
#define NICSH_DEF_IO_NUM_PDNG_NBUCKETS  18

struct niosd_io_ctx_stats
{
    enum niosd_io_ctx_stats_hist niocs_stat_type;
    struct binary_hist           niocs_bh;
    struct lreg_node             niocs_lrn;
};

struct niosd_io_ctx
{
    uint32_t                         nioctx_use_blocking_mode:1;
    struct ev_pipe                   nioctx_evp;
    enum niosd_io_ctx_type           nioctx_type;
    io_context_t                     nioctx_ctx;
    struct thread_ctl                nioctx_thr_ctl;
    struct niosd_io_ctx_stats        nioctx_stats[NICSH_IO_CTX_STATS_MAX];
    struct niosd_io_compl_event_ring nioctx_cer;
    struct lreg_node                 nioctx_lreg_node;
};

static inline struct binary_hist *
niosd_io_ctx_2_stats_bh(struct niosd_io_ctx *nioctx,
                        enum niosd_io_ctx_stats_hist counter)
{
    return &nioctx->nioctx_stats[counter].niocs_bh;
}

struct sb_header_data;

#define MAX_NIOSD_DEVICE_NAME (LREG_VALUE_STRING_MAX - 5)

struct niosd_device
{
    char                             ndev_name[MAX_NIOSD_DEVICE_NAME + 1];
    struct sb_header_data           *ndev_sb;
    struct stat                      ndev_stb;
    int                              ndev_fd;
    enum niosd_dev_status            ndev_status;
    struct niosd_io_ctx              ndev_ctxs[NIOSD_IO_CTX_TYPE_MAX];
};

static inline void
niosd_device_params_enable_blocking_mode(struct niosd_device *ndev,
                                         enum niosd_io_ctx_type nioctx_type)
{
    if (ndev)
        ndev->ndev_ctxs[nioctx_type].nioctx_use_blocking_mode = 1;
}

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

enum niosd_io_request_timers
{
    NIOSD_IO_REQ_TIMER_SUBMITTED = 0,
    NIOSD_IO_REQ_TIMER_EVENT_REAPED = 1,
    NIOSD_IO_REQ_TIMER_CB_EXEC = 2,
    NIOSD_IO_REQ_TIMER_ALL = 3,
};

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
    uint16_t                   niorq_nsectors;
    uint16_t                   niorq_compl_ev_done:1;
    enum niosd_io_request_type niorq_type;
    union
    {
        void                  *niorq_sink_buf;
        const void            *niorq_src_buf;
    };
    long                       niorq_res; // from linux aio
    long                       niorq_res2;
    niosd_io_callback_t        niorq_cb;
    void                      *niorq_cb_data;
    struct timespec            niorq_timers[NIOSD_IO_REQ_TIMER_ALL];
};

#define DBG_NIOSD_REQ(log_level, iorq, fmt, ...)                        \
    log_msg(log_level,                                                  \
            "iorq@%p %s(%zu:%zu) pblk:%x ns:%u t:%d ev=%u buf:%p "      \
            "res:%ld,%ld "fmt,                                          \
            (iorq), niosd_ctx_to_device((iorq)->niorq_ctx)->ndev_name,  \
            niosd_ctx_pending_io_ops((iorq)->niorq_ctx),                \
            niosd_ctx_pending_completion_ops((iorq)->niorq_ctx),        \
            (iorq)->niorq_pblk_id, (iorq)->niorq_nsectors,              \
            (iorq)->niorq_type, (iorq)->niorq_compl_ev_done,            \
            (iorq)->niorq_src_buf, (iorq)->niorq_res,                   \
            (iorq)->niorq_res2, ##__VA_ARGS__)

#define NIOSD_REQ_FATAL_IF(cond, iorq, message, ...)                   \
    if ((cond))                                                        \
    {                                                                  \
        DBG_NIOSD_REQ(LL_FATAL, iorq, message, ##__VA_ARGS__);         \
    }

static inline bool
niorq_has_error(const struct niosd_io_request *niorq)
{
    return (niorq->niorq_res2 ||
            (niorq->niorq_res &&
             niorq->niorq_res != (niorq->niorq_nsectors *
                                  NIOVA_SECTOR_SIZE))) ? true : false;
}

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

static inline size_t
niosd_io_request_nsectors_to_bytes(const struct niosd_io_request *niorq)
{
    return niosd_nsectors_to_bytes(niorq->niorq_nsectors);
}

static inline void
niosd_io_request_time_stamp(struct niosd_io_request *niorq,
                            const enum niosd_io_request_timers timer)
{
    niova_unstable_clock(&niorq->niorq_timers[timer]);
}

static inline void
niosd_io_request_time_stamp_apply(struct niosd_io_request *niorq,
                                  const enum niosd_io_request_timers timer,
                                  const struct timespec ts)
{
    niorq->niorq_timers[timer] = ts;
}

static inline long long
niosd_io_request_time_stamp_to_usec(struct niosd_io_request *niorq,
                                     enum niosd_io_request_timers ts)
{
    return timespec_2_usec(&niorq->niorq_timers[ts]);
}

static inline int
niosd_io_request_latency_stages_usec(struct niosd_io_request *niorq,
                                     enum niosd_io_request_timers start,
                                     enum niosd_io_request_timers end,
                                     long long *value)
{
    if (!niorq || !value || start >= end)
        return -EINVAL;

    *value = MAX(0, (niosd_io_request_time_stamp_to_usec(niorq, end) -
                     niosd_io_request_time_stamp_to_usec(niorq, start)));

    return 0;
}

niosd_io_submitter_ctx_t
niosd_device_params_init(const char *, struct niosd_device *);

niosd_io_submitter_ctx_int_t
niosd_device_open(struct niosd_device *);

niosd_io_submitter_ctx_int_t
niosd_device_close(struct niosd_device *);

void
niosd_io_request_destroy(struct niosd_io_request *);

niosd_io_submitter_ctx_int_t
niosd_io_request_init(struct niosd_io_request *, struct niosd_io_ctx *,
                      pblk_id_t, enum niosd_io_request_type, uint32_t, void *,
                      niosd_io_callback_t, void *);

niosd_io_submitter_ctx_int_t
niosd_io_submit(struct niosd_io_request **, long int);

niosd_io_completion_cb_ctx_size_t
niosd_io_events_complete(struct niosd_io_ctx *, long int);

int
nioctx_blocking_mode_fd_get(const struct niosd_io_ctx *nioctx);

#endif
