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

#include "niosd_io.h"

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

    return niosd_device_close(&ndev);
}
