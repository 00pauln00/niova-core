/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */
#include <time.h>
#include <stdio.h>

#include "common.h"
#include "log.h"
#include "random.h"

#define DEF_ITER 200000000
#define PRIME 1040071U
#define SMALL_PRIME 7879U

static void
simple_random(void)
{
    static unsigned int foo;
    foo += get_random();

    (void)foo;
}

static void
simple_clock_gettime_mono(void)
{
    static struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    (void)ts;
}

static void
simple_clock_gettime_mono_raw(void)
{
    static struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);

    (void)ts;
}

static void
simple_modulus(void)
{
    static unsigned long long mod_val = -1ULL;

    mod_val = mod_val - SMALL_PRIME + mod_val % PRIME;

    (void)mod_val;
}

static void
simple_addition(void)
{
    static unsigned long long val = PRIME;
    val += 1;

    (void)val;
}

static void
simple_multiply(void)
{
    static unsigned long long val = SMALL_PRIME;
    val *= SMALL_PRIME;

    (void)val;
}

static void
run_micro(void (*func)(void), size_t iterations, const char *name)
{
    struct timespec ts[2];
    niova_unstable_clock(&ts[0]);

    for (size_t i = 0; i < iterations; i++)
        func();

    niova_unstable_clock(&ts[1]);

    timespecsub(&ts[1], &ts[0], &ts[0]);

    unsigned long long num_nsecs = timespec_2_nsec(&ts[0]);

    fprintf(stdout, "%.03f\t\t%s\n",
            (float)num_nsecs / (float)iterations, name);
}

int
main(void)
{
    fprintf(stdout, "NS/OP\t\tTest Name\n"
            "----------------------------------------------\n");

    run_micro(simple_addition, DEF_ITER, "simple_addition");
    run_micro(simple_multiply, DEF_ITER, "simple_multiply");
    run_micro(simple_modulus, 10000, "simple_modulus");

    run_micro(simple_random, DEF_ITER, "random-number-generate");

    run_micro(simple_clock_gettime_mono, DEF_ITER / 5,
              "clock_gettime_monotonic");
    run_micro(simple_clock_gettime_mono_raw, DEF_ITER / 20,
              "clock_gettime_monotonic_raw");
    return 0;
}
