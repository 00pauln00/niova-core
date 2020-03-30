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

    fprintf(stdout, "-- Global Generator --\n");
    for (uint64_t i = 0; i < seqno; i++)
    {
        val = random_get();
        if (verbose)
            fprintf(stdout, "\ti=%04lx seqno=%lu rand-val=%08u\n",
                    i, seqno, val);
    }

    fprintf(stdout, "seqno=%lu rand-val=%08u\n", seqno, val);

    fprintf(stdout, "-- Local Generator --\n");

    struct random_data rand_data = {0};
    char rand_state_buf[RANDOM_STATE_BUF_LEN] = {0};

    if (initstate_r(1040071U, rand_state_buf,
                    RANDOM_STATE_BUF_LEN, &rand_data))
	SIMPLE_LOG_MSG(LL_FATAL, "initstate_r() failed: %s", strerror(errno));

    for (uint64_t i = 0; i < seqno; i++)
    {
        if (random_r(&rand_data, (int *)&val))
            SIMPLE_LOG_MSG(LL_FATAL, "random_r() failed: %s", strerror(errno));

        if (verbose)
        {
            fprintf(stdout, "\ti=%04lx seqno=%lu rand-val=%08u\n",
                    i, seqno, val);
        }
    }
    fprintf(stdout, "seqno=%lu rand-val=%08u\n", seqno, val);

    fprintf(stdout, "-- Local Generator Clone --\n");

    if (initstate_r(1040071U, rand_state_buf,
                    RANDOM_STATE_BUF_LEN, &rand_data))
	SIMPLE_LOG_MSG(LL_FATAL, "initstate_r() failed: %s", strerror(errno));

    struct random_data rand_data_clone = {0};
    char tmp[RANDOM_STATE_BUF_LEN] = {0};
    char rand_state_buf_clone[RANDOM_STATE_BUF_LEN] = {0};

    if (initstate_r(1040071U, tmp,
                    RANDOM_STATE_BUF_LEN, &rand_data_clone))
	SIMPLE_LOG_MSG(LL_FATAL, "initstate_r() failed: %s", strerror(errno));

    val = 0;
    unsigned int clone_val = 0;
    for (uint64_t i = 0; i < seqno; i++)
    {
        if (i == seqno / 2)
        {
//            memcpy(&rand_data_clone, &rand_data, sizeof(struct random_data));
            memcpy(rand_state_buf_clone, rand_state_buf, RANDOM_STATE_BUF_LEN);
            if (setstate_r(rand_state_buf_clone, &rand_data_clone))
                SIMPLE_LOG_MSG(LL_FATAL, "setstate_r() failed: %s",
                               strerror(errno));
        }

        if (random_r(&rand_data, (int *)&val))
            SIMPLE_LOG_MSG(LL_FATAL, "random_r() failed: %s", strerror(errno));

        if (i >= seqno / 2)
            if (random_r(&rand_data_clone, (int *)&clone_val))
                SIMPLE_LOG_MSG(LL_FATAL, "random_r() failed: %s",
                               strerror(errno));

        if (verbose)
        {
            if (i >= seqno / 2)
                fprintf(stdout,
                        "\ti=%04lx seqno=%lu rand-val=%08u\tclone-val=%08u\n",
                        i, seqno, val, clone_val);
            else
                fprintf(stdout, "\ti=%04lx seqno=%lu rand-val=%08u\n",
                        i, seqno, val);
        }
    }

    fprintf(stdout, "seqno=%lu rand-val=%08u clone-val=%08u\n",
            seqno, val, clone_val);
}
