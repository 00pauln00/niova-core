/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE 1
#include <pthread.h>
#include <libaio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "common.h"
#include "util.h"
#include "log.h"
#include "env.h"

#include "thread.h"
#include "niosd_io.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define NIOSD_LINUX_PROC_AIO_MAX "/proc/sys/fs/aio-max-nr"
#define NIOSD_IO_OPEN_FLAGS (O_DIRECT | O_RDWR)
#define NIOSD_MIN_DEVICE_SZ_IN_PBLKS 8192UL
#define NIOSD_MIN_DEVICE_SZ_IN_BYTES                    \
    (NIOSD_MIN_DEVICE_SZ_IN_PBLKS * PBLK_SIZE_BYTES)

static size_t niosdMaxAioEvents;
static size_t niosdMaxAioNreqsSubmit;

static size_t
niosd_num_aio_events_from_env(void)
{
    size_t env_num_aio_events = 0;
    char *env_num_aio_events_str = getenv(NIOVA_NUM_AIO_EVENTS_ENV);

    if (env_num_aio_events_str)
    {
        env_num_aio_events = atoll(env_num_aio_events_str);
        if (env_num_aio_events > NIOSD_MAX_AIO_EVENTS)
        {
            SIMPLE_LOG_MSG(LL_WARN, "env variable %s value exceeds max (%u)",
                           NIOVA_NUM_AIO_EVENTS_ENV, NIOSD_MAX_AIO_EVENTS);

            env_num_aio_events = NIOSD_MAX_AIO_EVENTS;
        }
        else if (env_num_aio_events < NIOSD_MIN_AIO_EVENTS)
        {
            SIMPLE_LOG_MSG(LL_WARN,
                           "env variable %s (%s) value below minumum (%u)",
                           NIOVA_NUM_AIO_EVENTS_ENV, env_num_aio_events_str,
                           NIOSD_MIN_AIO_EVENTS);

            env_num_aio_events = NIOSD_MIN_AIO_EVENTS;
        }
    }

    return env_num_aio_events;
}

static size_t
niosd_max_aio_events_from_proc(void)
{
    size_t proc_max_events = NIOSD_LOWER_AIO_EVENTS; //conservative guess
    FILE *proc_max_aio_fp = fopen(NIOSD_LINUX_PROC_AIO_MAX, "r");

    if (proc_max_aio_fp)
    {
        if (!fscanf(proc_max_aio_fp, "%zu", &proc_max_events))
            SIMPLE_LOG_MSG(LL_ERROR, "fscanf(%s): returns 0",
                           NIOSD_LINUX_PROC_AIO_MAX);

        if (fclose(proc_max_aio_fp))
            SIMPLE_LOG_MSG(LL_ERROR, "fclose(%s):  %s",
                           NIOSD_LINUX_PROC_AIO_MAX, strerror(errno));
    }
    else
    {
        SIMPLE_LOG_MSG(LL_ERROR, "fopen(%s) failed: %s",
                       NIOSD_LINUX_PROC_AIO_MAX, strerror(errno));
    }

    return proc_max_events;
}

static void
niosd_set_num_aio_events(void)
{
    if (niosdMaxAioEvents && niosdMaxAioNreqsSubmit)
        return;

    const int num_aio_ctxs = NIOSD_IO_CTX_TYPE_MAX - NIOSD_IO_CTX_TYPE_DEFAULT;
    const size_t env = niosd_num_aio_events_from_env();
    const size_t proc = niosd_max_aio_events_from_proc();

    /* Use 'env' if it was specified but ensure it does not exceed 'proc'.
     */
    const size_t num_aio_events =
        MIN((env ? env : NIOSD_DEFAULT_AIO_EVENTS), proc);

    SIMPLE_LOG_MSG(LL_WARN,
                   "nctxs=%d;  num-aio: env=%zu, proc=%zu, default=%u",
                   num_aio_ctxs, env, proc, NIOSD_DEFAULT_AIO_EVENTS);

    if (num_aio_events < NIOSD_MIN_AIO_EVENTS)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "num_aio_events=%zu is too small to proceed.",
                       num_aio_events);
        EXIT_ERROR_MSG(1, "Please increase %s to at least %d and restart.",
                       NIOSD_LINUX_PROC_AIO_MAX, NIOSD_MIN_AIO_EVENTS);
    }

    niosdMaxAioEvents = num_aio_events / num_aio_ctxs;
    niosdMaxAioNreqsSubmit = MIN(niosdMaxAioEvents,
                                 NIOSD_MAX_AIO_NREQS_SUBMIT);

    SIMPLE_LOG_MSG(LL_WARN, "niosdMaxAioEvents=%zu niosdMaxAioNreqsSubmit=%zu",
                   niosdMaxAioEvents, niosdMaxAioNreqsSubmit);
}

/**
 * niosd_device_params_init - initialize the provided device structure with
 *    the device name.
 * @dev_name:  pathname to the device.
 * @ndev:  device structure.
 */
niosd_io_submitter_ctx_t
niosd_device_params_init(const char *dev_name, struct niosd_device *ndev)
{
    memset(ndev, 0, sizeof(*ndev));

    strncpy(ndev->ndev_name, dev_name, PATH_MAX);

    ndev->ndev_fd = -1;
    ndev->ndev_status = NIOSD_DEV_STATUS_STARTUP_DEV_OPEN;
}

static niosd_io_event_ctx_bool_t
niosd_event_thread_should_continue(const struct niosd_io_ctx *nioctx)
{
    const enum niosd_dev_status ndev_status =
        niosd_ctx_to_device((struct niosd_io_ctx *)nioctx)->ndev_status;

    if (ndev_status == NIOSD_DEV_STATUS_RUNNING)
        return true;

    /* Device wants to stop.  Try to complete the remaining operations.
     */
    else if (ndev_status == NIOSD_DEV_STATUS_STOPPED &&
             niosd_ctx_total_pending_ops(nioctx))
        return true;

    return false;
}

// head == tail means that the ring is empty
static niosd_io_event_ctx_int_t
niosd_io_compl_event_ring_get_to_fill(struct niosd_io_ctx *nioctx,
                                      struct io_event **events)
{
    if (!events)
        return -EINVAL;

    *events = NULL;

    ssize_t npending_io_ops = niosd_ctx_pending_io_ops(nioctx);
    NIOVA_ASSERT(npending_io_ops >= 0);

    size_t nevents_to_fill = MAX(1, npending_io_ops);

    struct niosd_io_compl_event_ring *cer = &nioctx->nioctx_cer;

    int64_t head = niosd_ctx_to_cer_counter(nioctx, head);
    int64_t tail = niosd_ctx_to_cer_counter(nioctx, tail);

    NIOVA_ASSERT(head >= tail);

    /* Convert absolute counts relative to the array size.  It's permissible
     * for the relative tail to be > the head.
     */
    head = head % cer->niocer_num_events;
    tail = tail % cer->niocer_num_events;

    if (tail > head) // Don't overwrite the tail slot
        nevents_to_fill = MIN(nevents_to_fill, tail - head - 1);

    if (nevents_to_fill)
    {
        *events = &cer->niocer_events[head];

        nevents_to_fill =
            MIN(nevents_to_fill, cer->niocer_num_events - head);
    }

    return nevents_to_fill;
}

static niosd_io_event_ctx_t
niosd_device_event_thread_sleep(void)
{
    usleep(100);
}

static niosd_io_event_ctx_int64_t
niosd_device_event_ring_get_next_to_fill(struct niosd_io_ctx *nioctx,
                                         struct io_event **events_head)
{
    long int num_events_to_get = 0;

    while (!num_events_to_get)
    {
        num_events_to_get =
            niosd_io_compl_event_ring_get_to_fill(nioctx, events_head);

        if (!num_events_to_get)
            niosd_device_event_thread_sleep();
    }

    NIOVA_ASSERT(num_events_to_get > 0);

    return num_events_to_get;
}

static niosd_io_event_ctx_t
niosd_device_event_thread_iter_new_events(const struct io_event *events_head,
                                          const int num_events_completed)
{
    struct timespec now;
    niova_unstable_clock(&now);

    int i;
    for (i = 0; i < num_events_completed; i++)
    {
        struct niosd_io_request *niorq = events_head[i].data;
        niosd_io_request_time_stamp_apply(niorq,
                                          NIOSD_IO_REQ_TIMER_EVENT_REAPED,
                                          now);

        DBG_NIOSD_REQ(LL_DEBUG, niorq, "");
    }
}

static niosd_io_event_ctx_t
niosd_device_event_thread_post_new_events(struct niosd_io_ctx *nioctx,
                                          struct io_event *events_head,
                                          const int num_events_completed)
{
    niosd_device_event_thread_iter_new_events(events_head,
                                              num_events_completed);

    /* Notify completion ctx that new events have been obtained.
     */
    niosd_ctx_increment_cer_counter(nioctx, head, num_events_completed);
}

static niosd_io_event_ctx_int_t
niosd_device_event_thread_getevents(struct niosd_io_ctx *nioctx)
{
    int rc = 0;

    struct timespec ts = {.tv_sec = 0, .tv_nsec = 100000000};

    struct io_event *events_head;

    long int num_events_to_get =
        niosd_device_event_ring_get_next_to_fill(nioctx, &events_head);

    int num_events_completed =
        io_getevents(nioctx->nioctx_ctx, NIOSD_GETEVENTS_MIN,
                     num_events_to_get, &events_head[0], &ts);

    log_msg(LL_TRACE, "completed=%d max_to_get=%ld event_buf=%p",
            num_events_completed, num_events_to_get, &events_head[0]);

    if (num_events_completed > 0)
    {
        NIOVA_ASSERT(num_events_completed <= num_events_to_get);

        niosd_device_event_thread_post_new_events(nioctx, events_head,
                                                  num_events_completed);
    }
    else if (num_events_completed < 0) // Error case
    {
        rc = (num_events_completed == -EINTR) ? 0 : num_events_completed;
    }

    return rc;
}

static niosd_io_event_ctx_t *
niosd_device_event_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    NIOVA_ASSERT(tc);

    struct niosd_io_ctx *nioctx = tc->tc_arg;
    NIOVA_ASSERT(nioctx);

    log_msg(LL_DEBUG, "starting");

    long int rc;

    THREAD_LOOP_WITH_CTL(tc)
    {
        rc = niosd_device_event_thread_getevents(nioctx);

        if (rc || !niosd_event_thread_should_continue(nioctx))
            break;
    }

    log_msg(LL_DEBUG, "stopping");

    return (void *)rc;
}

static int
niosd_ctx_event_thread_start(struct niosd_io_ctx *nioctx)
{
    NIOVA_ASSERT(niosd_ctx_to_device(nioctx)->ndev_status ==
                 NIOSD_DEV_STATUS_STARTUP_DEV_OPEN);

    DECL_AND_FMT_STRING(thr_name, MAX_THREAD_NAME, "niosd_event.%c",
                        niosd_io_ctx_type_to_char(nioctx->nioctx_type));

    int rc = thread_create(niosd_device_event_thread, &nioctx->nioctx_thr_ctl,
                           thr_name, (void *)nioctx, NULL);

    if (!rc)
        thread_ctl_run(&nioctx->nioctx_thr_ctl);

    return -rc;
}

/**
 * niosd_io_ctx_setup - called by users of the NIOSD's I/O subsystem to prepare
 *    an asynchronous I/O context.  This function must be called before an
 *    niosd user may initiate I/O with the device.
 * @nioctx:  pointer to context storage.
 */
static int
niosd_io_ctx_setup(struct niosd_io_ctx *nioctx, enum niosd_io_ctx_type type)
{
    nioctx->nioctx_type = type;

    memset(&nioctx->nioctx_ctx, 0, sizeof(io_context_t));

    return io_setup(niosdMaxAioEvents, &nioctx->nioctx_ctx);
}

static int
niosd_io_completion_event_ring_init(struct niosd_io_compl_event_ring *cer,
                                    size_t num_events)
{
    CONST_OVERRIDE(size_t, cer->niocer_num_events, num_events);
    cer->niocer_events = niova_calloc(num_events, sizeof(struct io_event));

    return !cer->niocer_events ? -errno : 0;
}

static void
niosd_io_completion_event_ring_destroy(struct niosd_io_compl_event_ring *cer)
{
    log_msg(LL_DEBUG, "");

    niova_free(cer->niocer_events);

    cer->niocer_events = NULL;

    CONST_OVERRIDE(size_t, cer->niocer_num_events, 0);
}

static int
niosd_device_ctxs_init(struct niosd_device *ndev)
{
    enum niosd_io_ctx_type i;

    for (i = NIOSD_IO_CTX_TYPE_DEFAULT; i < NIOSD_IO_CTX_TYPE_MAX; i++)
    {
        struct niosd_io_ctx *nioctx = niosd_device_to_ctx(ndev, i);
        struct niosd_io_compl_event_ring *cer = &nioctx->nioctx_cer;

        int rc = niosd_io_completion_event_ring_init(cer, niosdMaxAioEvents);
        if (rc)
        {
            log_msg(LL_ERROR, "niosd_io_completion_event_ring_init(): %s",
                    strerror(-rc));
            return rc;
        }

        rc = niosd_io_ctx_setup(nioctx, i);
        if (rc)
        {
            log_msg(LL_ERROR, "niosd_io_ctx_setup(): %s", strerror(-rc));
            return rc;
        }

        rc = niosd_ctx_event_thread_start(nioctx);
        FATAL_IF(rc, "niosd_ctx_event_thread_start(): %s", strerror(-rc));
    }

    return 0;
}

/**
 * niosd_device_close - shutdown the given storage device.
 * @ndev:  pointer to the open storage device.
 */
niosd_io_submitter_ctx_int_t
niosd_device_close(struct niosd_device *ndev)
{
    if (ndev->ndev_status != NIOSD_DEV_STATUS_RUNNING &&
        ndev->ndev_status != NIOSD_DEV_STATUS_STARTUP_DEV_OPEN)
        return -EINVAL;

    const enum niosd_dev_status ndev_status = ndev->ndev_status;

    ndev->ndev_status = NIOSD_DEV_STATUS_STOPPED;

    if (ndev_status == NIOSD_DEV_STATUS_RUNNING)
    {
        enum niosd_io_ctx_type i;
        for (i = NIOSD_IO_CTX_TYPE_DEFAULT; i < NIOSD_IO_CTX_TYPE_MAX; i++)
        {
            struct niosd_io_ctx *nioctx = niosd_device_to_ctx(ndev, i);

            thread_halt_and_destroy(&nioctx->nioctx_thr_ctl);
        }
    }

    int rc = close(ndev->ndev_fd);
    if (rc)
    {
        rc = errno;
        log_msg(LL_ERROR, ": %s", strerror(rc));
        ndev->ndev_status = NIOSD_DEV_STATUS_ERROR;
    }

    if (ndev_status == NIOSD_DEV_STATUS_RUNNING)
    {
        enum niosd_io_ctx_type i;
        for (i = NIOSD_IO_CTX_TYPE_DEFAULT; i < NIOSD_IO_CTX_TYPE_MAX; i++)
        {
            struct niosd_io_ctx *nioctx = niosd_device_to_ctx(ndev, i);
            niosd_io_completion_event_ring_destroy(&nioctx->nioctx_cer);
        }
    }

    return -rc;
}

/**
 * niosd_device_open - opens the local storage device for use by the system.
 * @ndev:  pointer to unopened device which has been initialized by
 *     niosd_device_params_init().
 */
int
niosd_device_open(struct niosd_device *ndev)
{
    niosd_set_num_aio_events();

    if (ndev->ndev_status != NIOSD_DEV_STATUS_STARTUP_DEV_OPEN)
    {
        log_msg(LL_ERROR, "%s: invalid device state", ndev->ndev_name);

        return -EALREADY;
    }

    ndev->ndev_fd = open(ndev->ndev_name, NIOSD_IO_OPEN_FLAGS);
    if (ndev->ndev_fd < 0)
    {
        int rc = errno;
        log_msg(LL_ERROR, "open() %s: %s", ndev->ndev_name, strerror(rc));

        return -rc;
    }

    if (fstat(ndev->ndev_fd, &ndev->ndev_stb) < 0)
    {
        int rc = errno;
        log_msg(LL_ERROR, "fstat() %s: %s", ndev->ndev_name, strerror(rc));

        niosd_device_close(ndev);

        return -rc;
    }

    if (ndev->ndev_stb.st_size < NIOSD_MIN_DEVICE_SZ_IN_BYTES)
    {
        log_msg(LL_ERROR, "%s is %zu bytes - min size=%llu",
                ndev->ndev_name, ndev->ndev_stb.st_size,
                NIOSD_MIN_DEVICE_SZ_IN_BYTES);

        niosd_device_close(ndev);

        return -ERANGE;
    }

    int rc = niosd_device_ctxs_init(ndev);
    if (rc)
        niosd_device_close(ndev); //XX should free everything!

    ndev->ndev_status = NIOSD_DEV_STATUS_RUNNING;

    return rc;
}

/**
 * niosd_io_request_init_aio_internal - performs the linux AIO portion of the
 *    I/O request initialization.
 * @req:  a semi-initialized request from niosd_io_request_init().
 */
static niosd_io_submitter_ctx_int_t
niosd_io_request_init_aio_internal(struct niosd_io_request *niorq)
{
    int rc = 0;

    int ndev_fd =
        niosd_device_to_fd(niosd_ctx_to_device(niorq->niorq_ctx));

    switch (niorq->niorq_type)
    {
    case NIOSD_REQ_TYPE_PREAD:
        io_prep_pread(&niorq->niorq_iocb, ndev_fd, niorq->niorq_sink_buf,
                      niosd_nsectors_to_bytes(niorq->niorq_nsectors),
                      niosd_pblk_to_offset(niorq->niorq_pblk_id));
        break;

    case NIOSD_REQ_TYPE_PWRITE:
        io_prep_pwrite(&niorq->niorq_iocb, ndev_fd,
                       (void *)niorq->niorq_src_buf,
                       niosd_nsectors_to_bytes(niorq->niorq_nsectors),
                       niosd_pblk_to_offset(niorq->niorq_pblk_id));
        break;

    case NIOSD_REQ_TYPE_FSYNC:
        io_prep_fsync(&niorq->niorq_iocb, ndev_fd);
        break;

    case NIOSD_REQ_TYPE_NOOP:
        niorq->niorq_iocb.aio_lio_opcode = IO_CMD_NOOP;
        break;

    case NIOSD_REQ_TYPE_DISCARD:
        /* This operation is not handled by the aio layer.
         */
        break;

    default:
        log_msg(LL_WARN, "op type=%d is not supported", niorq->niorq_type);
        rc = -EOPNOTSUPP;
        break;
    }

    /* Use struct iocb's data member for storing the original 'req'.
     */
    niorq->niorq_iocb.data = niorq;

    return rc;
}

/**
 * niosd_io_request_destroy - free request memory.
 */
void
niosd_io_request_destroy(struct niosd_io_request *niorq)
{
    if (niorq)
    {
        DBG_NIOSD_REQ(LL_DEBUG, niorq, "");

        if (niorq->niorq_sink_buf)
            niova_free(niorq->niorq_sink_buf);

        niova_free(niorq);
    }
}

/**
 * niosd_io_request_init - initializes a user I/O request.
 * @req:  pointer to the request.
 * @dev:  pointer to the device which will service the request.
 * @ctx:  ctx for the request.
 * @pblk_id:  the physical block id of the request.
 * @type:  the request type.
 * @nsectors:  the length of the request in 512-byte sectors (may be optional)
 * @buf:  the source or sink buffer (may be optional)
 * @niorq_cb:  function callback to be issued on completion.
 * @cb_data:  additional callback data.
 */
niosd_io_submitter_ctx_int_t
niosd_io_request_init(struct niosd_io_request *niorq,
                      struct niosd_io_ctx *nioctx, pblk_id_t pblk_id,
                      enum niosd_io_request_type type, uint32_t nsectors,
                      void *buf, niosd_io_callback_t niorq_cb, void *cb_data)
{
    if (!niorq || !nioctx || !niorq_cb || !nsectors)
        return -EINVAL;

    /* Check the device status
     */
    else if (niosd_ctx_to_device(nioctx)->ndev_status !=
             NIOSD_DEV_STATUS_RUNNING)
        return -EAGAIN;

    niorq->niorq_ctx = nioctx;
    niorq->niorq_pblk_id = pblk_id;
    niorq->niorq_type = type;
    niorq->niorq_nsectors = nsectors;
    niorq->niorq_compl_ev_done = 0;
    niorq->niorq_sink_buf = buf;
    niorq->niorq_cb = niorq_cb;
    niorq->niorq_cb_data = cb_data;

    memset(&niorq->niorq_timers, 0,
           sizeof(struct timespec) * NIOSD_IO_REQ_TIMER_ALL);

    DBG_NIOSD_REQ(LL_DEBUG, niorq, "");

    return niosd_io_request_init_aio_internal(niorq);
}

static niosd_io_submitter_ctx_int_t
niosd_io_submit_requests_prep(struct niosd_io_request **niorqs,
                              struct iocb **iocb_ptrs, long int nreqs,
                              struct niosd_io_ctx **nioctx)
{
    const struct niosd_io_ctx *first_nioctx = niorqs[0]->niorq_ctx;
    struct timespec now;

    niova_unstable_clock(&now);

    long int i;
    for (i = 0; i < nreqs; i++)
    {
        if (i > 0)
        {
            NIOVA_ASSERT(first_nioctx == niorqs[i]->niorq_ctx);
        }
        else if (niorqs[i]->niorq_type == NIOSD_REQ_TYPE_UNINIT ||
                 niorqs[i]->niorq_type == NIOSD_REQ_TYPE_FSYNC ||
                 niorqs[i]->niorq_type == NIOSD_REQ_TYPE_DISCARD)
        {
            return -EOPNOTSUPP;
        }

        iocb_ptrs[i] = &niorqs[i]->niorq_iocb;

        niosd_io_request_time_stamp_apply(niorqs[i],
                                          NIOSD_IO_REQ_TIMER_SUBMITTED, now);
    }

    /* Return back to the caller.
     */
    *nioctx = niorqs[0]->niorq_ctx;

    return 0;
}

/**
 * niosd_io_submit_aio_internal - helper function for linux aio io_submit().
 *    This routine deals with the implementation details of io_submit() which
 *    include the possibility of queuing, and not immediately accepting, an
 *    array of requests.
 * NOTES:  Each request in the array must belong to the same niorq_ctx.
 */
static niosd_io_submitter_ctx_int_t
niosd_io_submit_aio_internal(struct niosd_io_request **niorqs, long int nreqs)
{
    if (nreqs <= 0)
        return -EINVAL;

    else if (nreqs > niosdMaxAioNreqsSubmit)
        return -E2BIG;

    struct iocb *iocb_ptrs[nreqs];
    struct niosd_io_ctx *nioctx;

    int rc = niosd_io_submit_requests_prep(niorqs, iocb_ptrs, nreqs, &nioctx);
    if (rc != 0)
        return rc;

    /* Optimistically increment the 'nsub' count prior to any submission
     * attempt to avoid the 'head' counter racing ahead of 'nsub'.
     */
    niosd_ctx_increment_cer_counter(nioctx, nsub, nreqs);

    int nsubmitted;

    for (nsubmitted = 0; nsubmitted < nreqs;)
    {
        rc = io_submit(nioctx->nioctx_ctx, nreqs - nsubmitted,
                       &iocb_ptrs[nsubmitted]);
        if (rc >= 0)
        {
            nsubmitted += rc;
            log_msg(LL_DEBUG, "io_submit(): %d", rc);
        }
        else if (rc == -EINTR)
        {
            log_msg(LL_DEBUG, "io_submit(): %s", strerror(-rc));
            continue;
        }
        else if (rc == -EAGAIN)
        {
            log_msg(LL_WARN, "io_submit(): %s (remaining=%ld)",
                    strerror(-rc), nreqs - nsubmitted);
            /* The caller needs to complete some event processing to make
             * space.
             */
            break;
        }
        else
        {
            niosd_ctx_decrement_cer_counter(nioctx, nsub, nreqs - nsubmitted);
            log_msg(LL_ERROR, "io_submit(): %s", strerror(-rc));

            return -rc;
        }
    }

    return nsubmitted;
}

/**
 * niosd_io_submit - Public entry point for I/O request submission by the
 *   submitter thread.
 * @niorqs:  array of niosd_io_requests
 * @nreqs:  number of requests in the array.
 */
niosd_io_submitter_ctx_int_t
niosd_io_submit(struct niosd_io_request **niorqs, long int nreqs)
{
    return niosd_io_submit_aio_internal(niorqs, nreqs);
}

static void
noisd_io_request_complete(struct niosd_io_request *niorq,
                        const struct io_event *event,
                        const struct timespec now)
{
    niosd_io_request_time_stamp_apply(niorq,
                                      NIOSD_IO_REQ_TIMER_CB_EXEC, now);

    niorq->niorq_res = event->res;
    niorq->niorq_res2 = event->res2;

    const enum log_level log_level =
        (niorq->niorq_res || niorq->niorq_res2) ? LL_WARN : LL_DEBUG;

    DBG_NIOSD_REQ(log_level, niorq, "");

    NIOSD_REQ_FATAL_IF((niorq->niorq_compl_ev_done), niorq,
                       "niorq_compl_ev_done already set");

    niorq->niorq_compl_ev_done = 1;

    /* Execute the user callback.  *niorq may be freed after the cb.
     */
    niorq->niorq_cb(niorq);
}

niosd_io_completion_cb_ctx_size_t
niosd_io_events_complete(struct niosd_io_ctx *nioctx, long int max_events)
{
    const uint64_t tail_cnt = niosd_ctx_to_cer_counter(nioctx, tail);
    size_t nevents_processed = 0;

    struct timespec now;
    niova_unstable_clock(&now);

    long int i;
    for (i = 0; i < max_events && niosd_ctx_pending_completion_ops(nioctx);
         i++)
    {
        const int event_slot =
            (tail_cnt + i) % niosd_ctx_to_cer_memb(nioctx, num_events);

        struct io_event *event = niosd_ctx_to_cer_event(nioctx, event_slot);
        struct niosd_io_request *niorq = event->data;

        niosd_ctx_increment_cer_counter(nioctx, tail, 1);

        noisd_io_request_complete(niorq, event, now);

        nevents_processed++;
    }

    return nevents_processed;
}
