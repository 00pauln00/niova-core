/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include "log.h"
#include "common.h"
#include "niosd_io.h"
#include "superblock.h"

#define OPTS "mhi"

struct niova_ctl
{
    const char         *nc_dev_name;
    unsigned int        nc_dev_mkosd     : 1,
                        nc_dev_dump_info : 1;
    struct niosd_device nc_ndev;
};

static void
niova_ctl_help(const int error)
{
    fprintf(errno ? stderr : stdout,
            "niova_ctl [-h] [-m] [-i] <device-name>\n"
            "\t -h  (help)   Prints this help message.\n"
            "\t -m  (mk-osd) Initializes device as a NIOVA OSD.\n"
            "\t -i  (info)   Get device information.\n");

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
        case 'i':
            nc->nc_dev_dump_info = 1;
            break;
        default:
            niova_ctl_help(EINVAL);
            break;
        }
    }

    nc->nc_dev_name = argv[argc - 1];
}

static void
niova_ctl_dump_dev_info(const struct niova_ctl *nc)
{
    if (!nc)
        return;

    const struct niosd_device *ndev = &nc->nc_ndev;

    fprintf(stdout,
            "device_name:        %s\n"
            "status:             %s\n"
            "niosd_uuid:         %lx.%lx\n"
            "physical_size:      %ld\n"
            "provisioned_size:   %ld\n"
            "superblock_version: %d\n",
            ndev->ndev_name,
            niosd_dev_status_2_string(ndev->ndev_status),
            sb_2_niosd_id(ndev->ndev_sb, false),
            sb_2_niosd_id(ndev->ndev_sb, true),
            sb_2_phys_size_bytes(ndev->ndev_sb),
            sb_2_prov_size_bytes(ndev->ndev_sb),
            sb_2_version(ndev->ndev_sb));
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

    if (nc.nc_dev_dump_info)
        niova_ctl_dump_dev_info(&nc);

    rc = niosd_device_close(&nc.nc_ndev);

    return rc;
}
