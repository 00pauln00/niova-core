/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2020
 */

#include <unistd.h>

#include "random.h"
#include "common.h"
#include "log.h"

#define OPTS "s:hn:v"

static unsigned int seed = ID_ANY_32bit;
static uint64_t     seqno = 10000;
static bool         verbose = false;

static void
randtest_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s [-s seed-value] [-n sequence-val]\n", argv[0]);

    exit(error);
}

static void
randtest_getopt(int argc, char **argv)
{
    if (!argc || !argv)
	return;

    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 's':
            seed = atoi(optarg);
            break;
        case 'h':
            randtest_print_help(0, argv);
            break;
        case 'n':
            seqno = atoll(optarg);
            break;
        case 'v':
            verbose = true;
            break;
	default:
            randtest_print_help(EINVAL, argv);
            break;
        }
    }
}

int
main(int argc, char **argv)
{
    randtest_getopt(argc, argv);

    if (seed != ID_ANY_32bit)
    {
        int rc = random_init(seed);
        FATAL_IF((rc), "random_init():  %s", strerror(-rc));
    }

    unsigned int val = 0;

    for (uint64_t i = 0; i < seqno; i++)
    {
        val = random_get();
        if (verbose)
            fprintf(stdout, "\ti=%04lx seqno=%lu rand-val=%08u\n",
                    i, seqno, val);
    }

    fprintf(stdout, "seqno=%lu rand-val=%08u\n", seqno, val);
}
