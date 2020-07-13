/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <unistd.h>
#include <uuid/uuid.h>

#include "common.h"
#include "pumice_db_client.h"

#define OPTS "u:r:h"

const char *raft_uuid_str;
const char *my_uuid_str;
static void

pmdbtc_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r <UUID> -u <UUID>\n", argv[0]);

    exit(error);
}

static void
pmdbtc_getopt(int argc, char **argv)
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
            pmdbtc_print_help(0, argv);
	    break;
        default:
            pmdbtc_print_help(EINVAL, argv);
            break;
	}
    }

    if (!raft_uuid_str || !my_uuid_str)
	pmdbtc_print_help(EINVAL, argv);
}

int
main(int argc, char **argv)
{
    pmdbtc_getopt(argc, argv);

    pmdb_t pmdb = PmdbClientStart(raft_uuid_str, my_uuid_str);
    if (!pmdb)
        exit(-errno);

    sleep(12000);
    return 0;
}
