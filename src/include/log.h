/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_LOG_H
#define NIOVA_LOG_H 1

#include <stdio.h>
#include <pthread.h>

#include "common.h"
#include "local_registry.h"

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

struct log_entry_info
{
    enum log_level lei_level;
    int            lei_lineno;
    const char    *lei_func;
};

#define REGISTRY_ENTRY_FILE_GENERATE                                    \
    static struct lreg_node regFileEntry = {                            \
        .lrn_cb_arg = (void *)__FILE__,                                 \
        .lrn_node_type = LREG_NODE_TYPE_ARRAY,                          \
        .lrn_user_type = LREG_USER_TYPE_LOG,                            \
        .lrn_statically_allocated = 1,                                  \
        .lrn_cb = log_lreg_cb,                                          \
    }

#define REGISTY_ENTRY_FUNCTION_GENERATE(debug_level)                    \
    static struct log_entry_info logEntryInfo = {                       \
        .lei_level = debug_level,                                       \
        .lei_lineno = __LINE__,                                         \
        .lei_func = __func__,                                           \
    };                                                                  \
    static struct lreg_node logMsgLrn = {                               \
        .lrn_cb_arg = &logEntryInfo,                                    \
        .lrn_node_type = LREG_NODE_TYPE_UNSIGNED_VAL,                   \
        .lrn_user_type = LREG_USER_TYPE_LOG,                            \
        .lrn_statically_allocated = 1,                                  \
        .lrn_cb = log_lreg_cb,                                          \
    };                                                                  \
    int _node_install_rc = 0;                                           \
    if (lreg_node_needs_installation(&regFileEntry))                    \
    {                                                                   \
        _node_install_rc = lreg_node_install_in_root(&regFileEntry);    \
        NIOVA_ASSERT(!_node_install_rc ||                               \
                     _node_install_rc == -EALREADY);                    \
    }                                                                   \
    if (lreg_node_needs_installation(&logMsgLrn))                       \
    {                                                                   \
        _node_install_rc = lreg_node_install(&logMsgLrn,                \
                                             &regFileEntry);            \
        NIOVA_ASSERT(!_node_install_rc ||                               \
                     _node_install_rc == -EALREADY);                    \
    }                                                                   \

#define LOG_MSG(lvl, message, ...)                                      \
{                                                                       \
    REGISTY_ENTRY_FUNCTION_GENERATE(lvl);                               \
                                                                        \
    if (logEntryInfo.lei_level <= dbgLevel)                             \
    {                                                                   \
        struct timespec ts;                                             \
        niova_unstable_clock(&ts);                                      \
        fprintf(stderr, "<%ld.%lu:%s:%lx:%s@%d> " message "\n",         \
                ts.tv_sec, ts.tv_nsec,                                  \
                ll_to_string(lvl), thread_id_get(), __func__,           \
                __LINE__, ##__VA_ARGS__);                               \
        if (lvl == LL_FATAL)                                            \
            thread_abort();                                             \
    }                                                                   \
}

#define FATAL_MSG(message, ...)                                         \
{                                                                       \
    struct timespec ts;                                                 \
    niova_unstable_clock(&ts);                                          \
    fprintf(stderr, "<%ld.%lu:%s:%lx:%s@%d> " message "\n",             \
            ts.tv_sec, ts.tv_nsec,                                      \
            ll_to_string(LL_FATAL), thread_id_get(), __func__,          \
            __LINE__, ##__VA_ARGS__);                                   \
    thread_abort();                                                     \
}

#define NIOVA_ASSERT(cond)                                      \
{                                                               \
    if (!(cond))                                                \
        FATAL_MSG("failed assertion: "#cond);                   \
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

thread_id_t
thread_id_get(void);

void
thread_abort(void);

void
log_level_set(enum log_level);

int
log_lreg_cb(enum lreg_node_cb_ops, struct lreg_node *lrn, struct lreg_value *);

#endif //NIOVA_LOG_H
