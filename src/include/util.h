/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_UTIL_H
#define NIOVA_UTIL_H 1

#include <stdio.h>
#include <uuid/uuid.h>
#include <ctype.h>
#include <pthread.h>
#include <errno.h>

// Do not include "log.h" here!
#include "common.h"

#define CTIME_R_STR_LEN 26
#define MK_TIME_STR_LEN 65

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
    MY_FATAL_IF((rc < 0 || rc > len), "rc=%d, requested len=%zu", rc, len);  \
}

#define	DECL_AND_FMT_STRING_RET_LEN(name, len, ret_len, fmt, ...)       \
char name[len + 1];                                                     \
{                                                                       \
    ssize_t rc = snprintf(name, len, fmt, ##__VA_ARGS__);               \
    MY_FATAL_IF((rc < 0 || rc > len), "rc=%zd, requested len=%zu", rc, len); \
    *(ret_len) = rc;                                                     \
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

/**
 * niova_newline_to_string_terminator - chomps newlines from the end of the
 *   supplied string.
 */
static inline void
niova_newline_to_string_terminator(char *string, const size_t max_len)
{
    if (!string || !max_len)
        return;

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

static inline ssize_t
niova_string_find_next_instance_of_char(const char *string, char char_to_find,
                                        const size_t max_len)
{
    if (!string || !max_len)
        return (ssize_t)-EINVAL;

    ssize_t pos;
    for (pos = 0; pos < (ssize_t)max_len; pos++)
        if (string[pos] == char_to_find)
            break;

    return pos < max_len ? pos : (ssize_t)-ENOENT;
}

/**
 * niova_clear_whitespace_from_end_of_string - chomps whitespace from the end
 *   of the supplied string.
 */
static inline void
niova_clear_whitespace_from_end_of_string(char *string, const size_t max_len)
{
    if (!string || !max_len)
        return;

    ssize_t pos = strnlen(string, max_len) - 1;
    for (; pos >= 0; pos--)
    {
        if (!isspace(string[pos]))
            break;

        string[pos] = '\0';
    }
}

static inline size_t
niova_count_nulls_from_end_of_buffer(const char *buf, const size_t len)
{
    if (!buf || !len)
        return 0;

    ssize_t cnt = 0;
    for (ssize_t pos = len - 1; pos > 0; pos--)
    {
        if (buf[pos] != '\0')
            break;

        cnt++;
    }

    return cnt;
}

static inline void
niova_string_convert_null_to_space(char *string, const size_t max_len)
{
    if (!string || !max_len)
	return;

    for (size_t pos = 0; pos < (max_len - 1); pos++)
        if (string[pos] == '\0')
            string[pos] = ' ';
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
usec_2_nsec(unsigned long long msec)
{
    return (msec * 1000);
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

static inline void
usec_2_timespec(struct timespec *ts, unsigned long long usec)
{
    if (!ts)
        return;

    ts->tv_sec = usec / 1000000;
    ts->tv_nsec	= usec_2_nsec(usec % 1000000);
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

static inline bool
timespec_has_value(const struct timespec *ts)
{
    return (ts->tv_sec || ts->tv_nsec) ? true : false;
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

static inline time_t
niova_realtime_coarse_clock_get_sec(void)
{
    struct timespec now;
    niova_realtime_coarse_clock(&now);

    return now.tv_sec;
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

static inline void
niova_set_tz(const char *tz_value, bool overwrite)
{
    if (tz_value)
    {
        setenv("TZ", tz_value, overwrite);
        tzset();
    }
}

static inline int
niova_mk_time_string(time_t time, char *out_str, size_t out_str_len)
{
    struct tm tm = {0};

    localtime_r(&time, &tm);

    size_t rc = strftime(out_str, out_str_len, "%a %b %d %H:%M:%S %Z %Y", &tm);

    return rc == 0 ? -ENOSPC : 0;
}

static inline void
niova_mutex_lock(pthread_mutex_t *mutex)
{
    MY_FATAL_IF(pthread_mutex_lock(mutex), "pthread_mutex_lock(): %s",
                strerror(errno));
}

static inline void
niova_mutex_unlock(pthread_mutex_t *mutex)
{
    MY_FATAL_IF(pthread_mutex_unlock(mutex), "pthread_mutex_unlock(): %s",
                strerror(errno));
}

#define NIOVA_TIMEDWAIT_COND(cond, mutex, cond_var, timeout)            \
({                                                                      \
    int _wc_rc = 0;                                                     \
    niova_mutex_lock(mutex);                                            \
                                                                        \
    while (!_wc_rc && (cond))                                           \
        _wc_rc = pthread_cond_timedwait(cond_var, mutex, timeout);      \
                                                                        \
    niova_mutex_unlock(mutex);                                          \
    _wc_rc;                                                             \
})

#define NIOVA_WAIT_COND_LOCKED(cond, mutex, cond_var)   \
    while ((cond)) pthread_cond_wait(cond_var, mutex)

#define NIOVA_WAIT_COND(cond, mutex, cond_var)                          \
{                                                                       \
    niova_mutex_lock(mutex);                                            \
    NIOVA_WAIT_COND_LOCKED(cond, mutex, cond_var);                      \
    niova_mutex_unlock(mutex);                                          \
}

#define NIOVA_SET_COND_AND_WAKE_LOCKED(how, set_code_block, cond_var)   \
{                                                                       \
    set_code_block;                                                     \
    pthread_cond_## how (cond_var);                                     \
}

#define NIOVA_SET_COND_AND_WAKE(how, set_code_block, mutex, cond_var)   \
{                                                                       \
    niova_mutex_lock(mutex);                                            \
    NIOVA_SET_COND_AND_WAKE_LOCKED(how, set_code_block, cond_var);      \
    niova_mutex_unlock(mutex);                                          \
}

#define NIOVA_CRC_OBJ(obj, type, crc32_memb, extra_contents)            \
({                                                                      \
    const size_t _offset =                                              \
        (offsetof(struct type, crc32_memb) + sizeof(crc32_t));          \
    const unsigned char *_buf = (const unsigned char *)(obj) + _offset; \
    const int _crc_len = sizeof(struct type) - offset + extra_contents; \
                                                                        \
    (obj)->crc32_memb = niova_crc(_buf, _crc_len, 0);                   \
    (obj)->crc32_memb;                                                  \
})

#endif
