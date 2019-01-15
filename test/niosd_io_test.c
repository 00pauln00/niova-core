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

#include "log.h"
#include "local_registry.h"

#include "niosd_io.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define TEST_DEVICE_NAME "./test_niosd.device"
#define TEST_DEVICE_SIZE_IN_BYTES (1024ul * 1024ul * 1024ul)

static int
create_test_device(void)
{
    int fd = open(TEST_DEVICE_NAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0)
    {
        int rc = errno;
        log_msg(LL_ERROR, "open(%s): %s", TEST_DEVICE_NAME, strerror(rc));

        return -rc;
    }

    int rc = ftruncate(fd, (off_t)TEST_DEVICE_SIZE_IN_BYTES);
    if (rc < 0)
    {
        rc = errno;
        log_msg(LL_ERROR, "ftruncate(): %s", strerror(rc));
    }

    close(fd);

    return rc;
}

static void
submit_request_cb(struct niosd_io_request *niorq)
{
    log_msg(LL_DEBUG, "niorq=%p sink_buf=%p", niorq, niorq->niorq_sink_buf);
    niova_free(niorq->niorq_sink_buf);
    niova_free(niorq);
}

static int
submit_request(struct niosd_device *ndev)
{
    struct niosd_io_request *niorq =
        niova_malloc(sizeof(struct niosd_io_request));

    if (!niorq)
        return -errno;

    char *buf = niova_malloc(NIOVA_SECTOR_SIZE);
    if (!buf)
        return -errno;

    int rc =
        niosd_io_request_init(niorq,
                              niosd_device_to_ctx(ndev,
                                                  NIOSD_IO_CTX_TYPE_DEFAULT),
                              0, NIOSD_REQ_TYPE_PREAD, 1, buf,
                              submit_request_cb, NULL);
    if (rc)
        return rc;

    rc = niosd_io_submit(&niorq, 1);

    return rc == 1 ? 0 : rc;
}

static void
complete_request(struct niosd_device *ndev)
{
    size_t nevents_completed;
    struct niosd_io_ctx *nioctx =
        niosd_device_to_ctx(ndev, NIOSD_IO_CTX_TYPE_DEFAULT);

    for (nevents_completed = 0; nevents_completed < 1;)
        nevents_completed += niosd_io_events_complete(nioctx, 1);
}

int
main(void)
{
    log_level_set(LL_DEBUG);

    int rc = create_test_device();
    if (rc)
        exit(rc);

    struct niosd_device ndev;
    niosd_device_params_init(TEST_DEVICE_NAME, &ndev);

    rc = niosd_device_open(&ndev);
    if (rc)
        exit(rc);

    usleep(10000);

    int i;
    for (i = 0; i < 10; i++)
    {
        rc = submit_request(&ndev);
        if (rc)
            log_msg(LL_ERROR, "submit_request(): %s", strerror(-rc));
    }

    for (i = 0; i < 10; i++)
        complete_request(&ndev);

    return niosd_device_close(&ndev);
}
