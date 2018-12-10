/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE 1 // for O_DIRECT
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "log.h"

#include "niosd_io.h"

#define NIOSD_IO_OPEN_FLAGS (O_DIRECT | O_RDWR)
#define NIOSD_MIN_DEVICE_SZ_IN_PBLKS 8192UL
#define NIOSD_MIN_DEVICE_SZ_IN_BYTES                    \
    (NIOSD_MIN_DEVICE_SZ_IN_PBLKS * PBLK_SIZE_BYTES)

static size_t niosdMaxAioEvents = NIOSD_MAX_AIO_EVENTS;
static size_t niosdMaxAioNreqsSubmit = NIOSD_MAX_AIO_NREQS_SUBMIT;

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
    ndev->ndev_status = NIOSD_DEV_STATUS_STARTING;
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
        nevents_to_fill= MIN(nevents_to_fill, tail - head - 1);

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
    usleep(64);
}

// HERE we MUST get all the events that we claimed to get!
static niosd_io_event_ctx_int_t
niosd_device_event_thread_getevents(struct niosd_io_ctx *nioctx,
                                    struct io_event *event_head,
                                    const long int num_events_to_get)
{
    long int events_remaining = num_events_to_get;
    int rc = 0;
    struct timespec ts = {.tv_sec = 2, .tv_nsec = 0};

    do
    {
        long int idx = num_events_to_get - events_remaining;

        int num_events_completed =
            io_getevents(nioctx->nioctx_ctx, NIOSD_GETEVENTS_MIN,
                         events_remaining, &event_head[idx], &ts);

        log_msg(LL_DEBUG, "num_events_completed=%d", num_events_completed);

        if (rc > 0)
        {
            niosd_ctx_increment_cer_counter(nioctx, head,
                                            num_events_completed);

            NIOVA_ASSERT(events_remaining >= 0);
        }
        else
        {
            if (num_events_completed == -EINTR)
                continue;
            else
                rc = num_events_completed;
        }

    } while (events_remaining && niosd_event_thread_should_continue(nioctx));

    //XXX if / when an error is returned we need to get rid of all the pending
    //    requests in the device.
    return rc;
}

static niosd_io_event_ctx_t *
niosd_device_event_thread(void *arg)
{
    log_msg(LL_DEBUG, "starting");

    NIOVA_ASSERT(arg);

    struct niosd_io_ctx *nioctx = arg;

    long int rc = 0;

    do
    {
        struct io_event *events_head;
        long int num_events_to_get =
            niosd_io_compl_event_ring_get_to_fill(nioctx, &events_head);

        log_msg(LL_DEBUG, "num_events_to_get=%lu", num_events_to_get);

        if (num_events_to_get > 0)
            rc = niosd_device_event_thread_getevents(nioctx, events_head,
                                                     num_events_to_get);
        else
            niosd_device_event_thread_sleep();

        if (rc < 0)
            break;
    } while (niosd_event_thread_should_continue(nioctx));

    log_msg(LL_DEBUG, "stopping");

    return (void *)rc;
}

static int
niosd_ctx_event_thread_start(struct niosd_io_ctx *nioctx)
{
    NIOVA_ASSERT(niosd_ctx_to_device(nioctx)->ndev_status ==
                 NIOSD_DEV_STATUS_STARTING);

    int rc = pthread_create(&nioctx->nioctx_event_thread, NULL,
                            niosd_device_event_thread, (void *)nioctx);
    return rc;
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
        if (rc)
        {
            log_msg(LL_ERROR, "niosd_ctx_event_thread_start(): %s",
                    strerror(-rc));
            return rc;
        }
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
        ndev->ndev_status != NIOSD_DEV_STATUS_STARTING)
        return -EINVAL;

    const enum niosd_dev_status ndev_status = ndev->ndev_status;

    ndev->ndev_status = NIOSD_DEV_STATUS_STOPPED;

    if (ndev_status == NIOSD_DEV_STATUS_RUNNING)
    {
        enum niosd_io_ctx_type i;
        for (i = NIOSD_IO_CTX_TYPE_DEFAULT; i < NIOSD_IO_CTX_TYPE_MAX; i++)
        {
            void *ret;
            pthread_join(niosd_device_to_ctx(ndev, i)->nioctx_event_thread,
                         &ret);
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
    if (ndev->ndev_status != NIOSD_DEV_STATUS_STARTING)
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

    /* Use struct iocb's data member for storing the original 'req'.
     */
    niorq->niorq_iocb.data = niorq;

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

    return rc;
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
    if (!niorq || !nioctx || !niorq_cb)
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
    niorq->niorq_sink_buf = buf;
    niorq->niorq_cb = niorq_cb;
    niorq->niorq_cb_data = cb_data;

    DBG_NIOSD_REQ(LL_DEBUG, niorq, "");

    return niosd_io_request_init_aio_internal(niorq);
}

/**
 * niosd_io_submit_aio_internal - helper function for linux aio io_submit().
 *    This routine deals with the implementation details of io_submit() which
 *    include the possibility of queing, and not immediately accepting, an
 *    array of requests.
 */
static niosd_io_submitter_ctx_int_t
niosd_io_submit_aio_internal(struct niosd_io_request **niorqs, long int nreqs)
{
    if (nreqs > niosdMaxAioNreqsSubmit)
        return -E2BIG;

    else if (!nreqs)
        return -EINVAL;

    struct iocb *iocb_ptrs[nreqs];

    long int i;
    for (i = 0; i < nreqs; i++)
        iocb_ptrs[i] = &niorqs[i]->niorq_iocb;

    int nsubmitted;
    int nretries;
    int rc = 0;

    for (nsubmitted = 0, nretries = 0; nsubmitted < nreqs &&
             nretries < NIOSD_AIO_NRETRIES; nretries++)
    {
        int nreqs_this_iteration = nreqs - nsubmitted;
        int nsub_this_iteration =
            io_submit(niorqs[i]->niorq_ctx->nioctx_ctx, nreqs_this_iteration,
                      &iocb_ptrs[nsubmitted]);

        nsubmitted += nsub_this_iteration;

        rc = nreqs_this_iteration != nsub_this_iteration ? errno : 0;
        if (rc && rc != EAGAIN)
        {
            log_msg(LL_ERROR, "io_submit(): %s", strerror(errno));
            return -rc;
        }

        //XXXX
        ///is this correct -- maybe want to increment on success?
//        niosd_ctx_increment_cer_counter(&reqs[0]->niorq_ctx, nsub,
//                                        nreqs_this_iteration);
        if (rc)
        {
            log_msg(LL_WARN, "io_submit(): %s (retries=%d)",
                    strerror(errno), nretries + 1);

            /* Wait a little bit before retrying
             */
            usleep(NIOSD_AIO_RETRY_DELAY_USEC * (nretries + 1));
        }
    }

    return -rc;
}

static niosd_io_submitter_ctx_int_t
niosd_io_submit_requests_check(struct niosd_io_request **niorqs,
                               long int nreqs)
{
    if (nreqs <= 0)
        return -EINVAL;

    const struct niosd_io_ctx *first_nioctx = niorqs[0]->niorq_ctx;
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
    }

    return 0;
}

niosd_io_submitter_ctx_int_t
niosd_io_submit(struct niosd_io_request **niorqs, long int nreqs)
{
    int rc = niosd_io_submit_requests_check(niorqs, nreqs);
    if (rc)
        return rc;

    return niosd_io_submit_aio_internal(niorqs, nreqs);
}
