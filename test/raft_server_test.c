/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdio.h>
#include <unistd.h>

#include "raft_net.h"
#include "raft_test.h"

#define OPTS "u:r:h"

const char *raft_uuid_str;
const char *my_uuid_str;

static void
rst_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r UUID -n UUID\n", argv[0]);

    exit(error);
}

static void
rst_getopt(int argc, char **argv)
{
    if (!argc || !argv)
        return;

    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'r':
            raft_uuid_str = optarg;
            break;
        case 'u':
            my_uuid_str = optarg;
            break;
        case 'h':
            rst_print_help(0, argv);
            break;
        default:
            rst_print_help(EINVAL, argv);
            break;
        }
    }

    if (!raft_uuid_str || !my_uuid_str)
        rst_print_help(EINVAL, argv);
}

int
main(int argc, char **argv)
{
    rst_getopt(argc, argv);

    return raft_net_server_instance_run(raft_uuid_str, my_uuid_str);
}
