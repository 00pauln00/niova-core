/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include "log.h"
#include "common.h"
#include "niosd_io.h"

#define OPTS "mh"

struct niova_ctl
{
    const char         *nc_dev_name;
    unsigned int        nc_dev_mkosd:1;
    struct niosd_device nc_ndev;
};

static void
niova_ctl_help(const int error)
{
    fprintf(errno ? stderr : stdout,
            "niova_ctl [-h] [-m] <device-name>\n"
            "\t -h  help - prints this help message.\n"
            "\t -m  mk-osd - initializes device for niova OSD.\n");

    exit(error);

}

static void
niova_ctl_getopt(int argc, char **argv, struct niova_ctl *nc)
{
    int opt;
    int rem_args = argc - 1;

    if (rem_args < 1)
        return niova_ctl_help(EINVAL);

    while ((opt = getopt(argc, argv, OPTS)) != 1 && rem_args > 1)
    {
        rem_args--;

        switch (opt)
        {
        case 'm':
            nc->nc_dev_mkosd = 1;
            break;
        case 'h':
            niova_ctl_help(0);
            break;
        default:
            niova_ctl_help(EINVAL);
            break;
        }
    }

    nc->nc_dev_name = argv[argc - 1];
}

int
main(int argc, char **argv)
{
    struct niova_ctl nc = {0};

    niova_ctl_getopt(argc, argv, &nc);

    niosd_device_params_init(nc.nc_dev_name, &nc.nc_ndev);

    niosd_device_params_enable_blocking_mode(&nc.nc_ndev,
                                             NIOSD_IO_CTX_TYPE_USER);

    niosd_device_params_enable_blocking_mode(&nc.nc_ndev,
                                             NIOSD_IO_CTX_TYPE_SYSTEM);

    int rc = niosd_device_open(&nc.nc_ndev);
    if (rc)
        exit(rc);


    rc = niosd_device_close(&nc.nc_ndev);

    return rc;
}
