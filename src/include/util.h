/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_UTIL_H
#define NIOVA_UTIL_H 1

#include <stdio.h>
#include <uuid/uuid.h>

// Do not include "log.h" here!
#include "common.h"

#define CTIME_R_STR_LEN 26

#if defined MY_FATAL_IF
#undef MY_FATAL_IF
#endif

#define OFFSET_CAST(type, member, src_ptr)       \
    (struct type *)(((char *)(src_ptr)) - offsetof(struct type, member))

#define MY_FATAL_IF(cond, msg, ...)                     \
    if (cond)                                           \
    {                                                   \
        fprintf(stderr, "<%s@%d>" msg"\n",              \
                __func__, __LINE__, ##__VA_ARGS__);     \
        abort();                                        \
    }

#define	DECL_AND_FMT_STRING(name, len, fmt, ...)                \
char name[len + 1];                                             \
{                                                               \
    int rc = snprintf(name, len, fmt, ##__VA_ARGS__);           \
    MY_FATAL_IF((rc > len), "rc=%d, requested len=%zu", rc, len);  \
}

#define DECL_AND_INIT_STRING(name, str_len, init_char, init_char_len)   \
char name[str_len + 1] = {0};                                           \
{                                                                       \
    for (int i = 0; i < MIN(str_len, init_char_len); i++)               \
        name[i] = init_char;                                            \
}

#define DECLARE_AND_INIT_UUID_STR(name, uuid)   \
char name[UUID_STR_LEN];                        \
{                                               \
    uuid_unparse(uuid, name);                   \
}

static inline void
niova_uuid_2_uint64(const uuid_t uuid_in, uint64_t *high, uint64_t *low)
{
    if (high)
        *high = *(const unsigned long long *)((const char *)&uuid_in[0]);

    if (low)
        *low = *(const unsigned long long *)((const char *)&uuid_in[8]);
}

static inline void
niova_newline_to_string_terminator(char *string, const size_t max_len)
{
    ssize_t len = strnlen(string, max_len);

    for (ssize_t i = len - 1; i >=0; i--)
    {
        if (string[i] == '\n')
        {
            string[i] = '\0';
            break;
        }
    }
}

/**
 * clock_gettime() wrappers
 */
#define niova_unstable_clock(dest)                              \
    MY_FATAL_IF(clock_gettime(CLOCK_MONOTONIC, (dest)),         \
                "clock_gettime() %s", strerror(errno))

#define niova_unstable_coarse_clock(dest)                       \
    MY_FATAL_IF(clock_gettime(CLOCK_MONOTONIC_COARSE, (dest)),  \
                "clock_gettime() %s", strerror(errno))

#define niova_stable_clock(dest)                                \
    MY_FATAL_IF(clock_gettime(CLOCK_MONOTONIC_RAW, (dest)),     \
                "clock_gettime() %s", strerror(errno))

#define niova_realtime_clock(dest)                              \
    MY_FATAL_IF(clock_gettime(CLOCK_REALTIME, (dest)),          \
                "clock_gettime() %s", strerror(errno))

#define niova_realtime_coarse_clock(dest)                       \
    MY_FATAL_IF(clock_gettime(CLOCK_REALTIME_COARSE, (dest)),   \
                "clock_gettime() %s", strerror(errno))


/**
 * BSD timespec macros
 */
#ifndef timespecclear
#define timespecclear(tsp)		((tsp)->tv_sec = (tsp)->tv_nsec = 0)
#endif

#ifndef timespecisset
#define timespecisset(tsp)		((tsp)->tv_sec || (tsp)->tv_nsec)
#endif

#ifndef timespeccmp
#define timespeccmp(tsp, usp, cmp)					\
    (((tsp)->tv_sec == (usp)->tv_sec) ?                                 \
     ((tsp)->tv_nsec cmp (usp)->tv_nsec) :				\
     ((tsp)->tv_sec cmp (usp)->tv_sec))
#endif

#ifndef timespecadd
#define timespecadd(tsp, usp, vsp)					\
    do {								\
        (vsp)->tv_sec = (tsp)->tv_sec + (usp)->tv_sec;                  \
        (vsp)->tv_nsec = (tsp)->tv_nsec + (usp)->tv_nsec;               \
        if ((vsp)->tv_nsec >= 1000000000L) {                            \
            (vsp)->tv_sec++;                                            \
            (vsp)->tv_nsec -= 1000000000L;                              \
        }                                                               \
    } while (0)
#endif

#ifndef timespecsub
#define timespecsub(tsp, usp, vsp)					\
    do {								\
        (vsp)->tv_sec = (tsp)->tv_sec - (usp)->tv_sec;                  \
        (vsp)->tv_nsec = (tsp)->tv_nsec - (usp)->tv_nsec;               \
        if ((vsp)->tv_nsec < 0) {                                       \
            (vsp)->tv_sec--;                                            \
            (vsp)->tv_nsec += 1000000000L;                              \
        }                                                               \
    } while (0)
#endif

static inline void
timespec_clear(struct timespec *ts)
{
    if (ts)
        ts->tv_sec = ts->tv_nsec = 0;
}

static inline unsigned long long
msec_2_nsec(unsigned long long msec)
{
    return (msec * 1000000);
}

static inline unsigned long long
nsec_2_msec(unsigned long long nsec)
{
    return (nsec / 1000000);
}

static inline void
msec_2_timespec(struct timespec *ts, unsigned long long msec)
{
    if (!ts)
        return;

    ts->tv_sec = msec / 1000;
    ts->tv_nsec	= msec_2_nsec(msec % 1000);
}

static inline unsigned long long
timespec_2_nsec(const struct timespec *ts)
{
    return (ts->tv_sec * 1000000000) + ts->tv_nsec;
}

static inline unsigned long long
timespec_2_usec(const struct timespec *ts)
{
    return (ts->tv_sec * 1000000) + (ts->tv_nsec / 1000);
}

static inline unsigned long long
timespec_2_msec(const struct timespec *ts)
{
    return (ts->tv_sec * 1000) + (ts->tv_nsec / 1000000);
}

static inline float
timespec_2_float(const struct timespec *ts)
{
    return (float)ts->tv_sec + (.000000001 * (float)ts->tv_nsec);
}

static inline float
timeval_2_float(const struct timeval *tv)
{
    return (float)tv->tv_sec + (.000001 * (float)tv->tv_usec);
}

static inline unsigned long long
niova_unstable_coarse_clock_get_msec(void)
{
    struct timespec now;
    niova_unstable_coarse_clock(&now);

    return timespec_2_msec(&now);
}

static inline unsigned long long
niova_realtime_coarse_clock_get_msec(void)
{
    struct timespec now;
    niova_realtime_coarse_clock(&now);

    return timespec_2_msec(&now);
}

static inline unsigned long long
niova_unstable_coarse_clock_get_usec(void)
{
    struct timespec now;
    niova_unstable_coarse_clock(&now);

    return timespec_2_usec(&now);
}

static inline int
niova_string_to_bool(const char *string, bool *ret_bool)
{
    if (!strncmp(string, "true", 4))
        *ret_bool = true;
    else if (!strncmp(string, "false", 5))
        *ret_bool = false;
    else
        return -EINVAL;

    return 0;
}

#endif
