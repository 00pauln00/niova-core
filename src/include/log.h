/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_LOG_H
#define NIOVA_LOG_H 1

#include <stdio.h>
#include <pthread.h>

typedef pthread_t thread_id_t;

enum log_level
{
    LL_FATAL = 0,
    LL_ERROR = 1,
    LL_WARN  = 2,
    LL_DEBUG = 3,
    LL_TRACE = 4,
    LL_MAX
};

extern enum log_level dbgLevel;


static inline const char *
ll_to_string(enum log_level ll)
{
    switch (ll)
    {
    case LL_FATAL:
        return "fatal";
    case LL_ERROR:
        return "error";
    case LL_WARN:
        return "warn";
    case LL_TRACE:
        return "trace";
    default:
        break;
    }

    return "debug";
}

#define LOG_MSG(lvl, message, ...)                                      \
{                                                                       \
    if (lvl <= dbgLevel)                                                \
    {                                                                   \
        struct timespec ts;                                             \
        clock_gettime(CLOCK_MONOTONIC_RAW, &ts);                        \
        fprintf(stderr, "<%ld.%lu:%s:%lx:%s@%d> " message "\n",       \
                ts.tv_sec, ts.tv_nsec,                  \
                ll_to_string(lvl), thread_id_get(), __func__,           \
                __LINE__, ##__VA_ARGS__);                               \
        if (lvl == LL_FATAL)                                            \
            thread_abort();                                             \
    }                                                                   \
}

#define DEBUG_BLOCK(lvl)                        \
    if (lvl <= dbgLevel)

#define log_msg LOG_MSG

#define STDOUT_MSG(message, ...)                                      \
{                                                                     \
    fprintf(stdout, "<%lx:%s@%d> " message "\n",                      \
            thread_id_get(), __func__,                                \
            __LINE__,##__VA_ARGS__);                                  \
}

#define NIOVA_ASSERT(cond)                                      \
{                                                               \
    if (!(cond))                                                \
        LOG_MSG(LL_FATAL, "failed assertion: "#cond);           \
}

thread_id_t
thread_id_get(void);

void
thread_abort(void);

void
log_level_set(enum log_level);

#endif //NIOVA_LOG_H
