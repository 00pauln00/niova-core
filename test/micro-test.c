/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include "common.h"
#include "log.h"
#include "random.h"
#include "atomic.h"
#include "crc32.h"

#define DEF_ITER 200000000
#define PRIME 1040071U
#define SMALL_PRIME 7879U

size_t iterator;

static void
simple_noop(void)
{
    (void)iterator;
    return;
}

static void
simple_random(void)
{
    static unsigned int foo;
    foo += random_get();

    (void)foo;
}

static void
simple_clock_gettime_mono_coarse(void)
{
    static struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);

    (void)ts;
}

static void
simple_clock_gettime_realtime_coarse(void)
{
    static struct timespec ts;
    clock_gettime(CLOCK_REALTIME_COARSE, &ts);

    (void)ts;
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
simple_clock_gettime_realtime(void)
{
    static struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

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
atomic_op(bool inc_or_read)
{
    static niova_atomic64_t val = PRIME;
    static int64_t foo;

    foo = inc_or_read ?
        niova_atomic_inc(&val) : niova_atomic_read(&val);

    (void)val;
    (void)foo;
}

static void
atomic_addition(void)
{
    return atomic_op(true);
}

static void
atomic_read(void)
{
    return atomic_op(false);
}

static void
atomic_cas_noop(void)
{
    static niova_atomic64_t val = PRIME;
    niova_atomic_cas(&val, 0, 1);

    (void)val;
}

static void
atomic_cas(void)
{
    static niova_atomic64_t val = 0;
    niova_atomic_cas(&val, iterator, iterator + 1);

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
simple_uuid_generate(void)
{
    static uuid_t uuid;
    uuid_generate(uuid);
}
static void
simple_uuid_generate_time(void)
{
    static uuid_t uuid;
    uuid_generate_time(uuid);
}
static void
simple_uuid_generate_random(void)
{
    static uuid_t uuid;
    uuid_generate_random(uuid);
}

static void
simple_crc32_64byte_buf(void)
{
    static uint32_t val = PRIME;
    uint64_t buffer[8];
    buffer[0] = val;

#if defined(__x86_64__)
    val = crc_pcl((const unsigned char *)buffer, (sizeof(uint64_t) * 8),
                  0 ^ 0xFFFFFFFF) ^ 0xFFFFFFFF;
    (void)val;
#endif

    return;
}

static void
micro_pthread_self(void)
{
    static pthread_t val;
    val += pthread_self();
}

static void
run_micro(void (*func)(void), size_t iterations, const char *name)
{
    struct timespec ts[2];
    niova_unstable_clock(&ts[0]);

    for (iterator = 0; iterator < iterations; iterator++)
        func();

    niova_unstable_clock(&ts[1]);

    timespecsub(&ts[1], &ts[0], &ts[0]);

    unsigned long long num_nsecs = timespec_2_nsec(&ts[0]);

    fprintf(stdout, "%9.3f\t\t%s\n",
            (float)num_nsecs / (float)iterations, name);
}

int
main(void)
{
    fprintf(stdout, "    NS/OP\t\tTest Name\n"
                    "--------------------------------------------------\n");

    run_micro(simple_noop, DEF_ITER, "simple_noop");
    run_micro(simple_addition, DEF_ITER, "simple_addition");
    run_micro(simple_multiply, DEF_ITER, "simple_multiply");
    run_micro(simple_modulus, DEF_ITER / 5, "simple_modulus");
    run_micro(atomic_addition, DEF_ITER, "atomic_addition");
    run_micro(atomic_read, DEF_ITER, "atomic_read");
    run_micro(atomic_cas_noop, DEF_ITER / 5, "atomic_cas_noop");
    run_micro(atomic_cas, DEF_ITER / 5, "atomic_cas");
    run_micro(simple_random, DEF_ITER, "random_number_generate");
    run_micro(simple_crc32_64byte_buf, DEF_ITER / 10,
              "simple_crc32_64byte_buf");
    run_micro(simple_clock_gettime_mono_coarse, DEF_ITER,
              "clock_gettime_monotonic_coarse");
    run_micro(simple_clock_gettime_realtime_coarse, DEF_ITER,
              "clock_gettime_realtime_coarse");
    run_micro(simple_clock_gettime_mono, DEF_ITER / 20,
              "clock_gettime_monotonic");
    run_micro(simple_clock_gettime_realtime, DEF_ITER / 20,
              "clock_gettime_realtime");
    run_micro(simple_clock_gettime_mono_raw, DEF_ITER / 100,
              "clock_gettime_monotonic_raw");
    run_micro(simple_uuid_generate, DEF_ITER / 10000,
              "uuid_generate");
    run_micro(simple_uuid_generate_random, DEF_ITER / 10000,
              "uuid_generate_random");
    run_micro(simple_uuid_generate_time, DEF_ITER / 10000,
              "uuid_generate_time");
    run_micro(micro_pthread_self, DEF_ITER,
              "pthread_self");
    return 0;
}
