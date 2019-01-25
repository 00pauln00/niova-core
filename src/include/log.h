/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_LOG_H
#define NIOVA_LOG_H 1

#include <stdio.h>
#include <pthread.h>

#include "thread.h"
#include "common.h"
#include "util.h"
#include "local_registry.h"

LREG_ROOT_ENTRY_EXPORT(logEntries);

typedef pthread_t thread_id_t;

enum log_level
{
    LL_FATAL  = 0,
    LL_ERROR  = 1,
    LL_WARN   = 2,
    LL_NOTIFY = 3,
    LL_DEBUG  = 4,
    LL_TRACE  = 5,
    LL_MAX    = 6,
    LL_ANY    = LL_MAX,
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

#define REGISTY_ENTRY_FUNCTION_GENERATE                                 \
    static struct log_entry_info logEntryInfo = {                       \
        .lei_level = LL_ANY,                                            \
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
        _node_install_rc =                                              \
            lreg_node_install_prepare(&regFileEntry,                    \
                                      LREG_ROOT_ENTRY_PTR(logEntries)); \
        NIOVA_ASSERT(!_node_install_rc ||                               \
                     _node_install_rc == -EALREADY);                    \
    }                                                                   \
    if (lreg_node_needs_installation(&logMsgLrn))                       \
    {                                                                   \
        _node_install_rc = lreg_node_install_prepare(&logMsgLrn,        \
                                                     &regFileEntry);    \
        NIOVA_ASSERT(!_node_install_rc ||                               \
                     _node_install_rc == -EALREADY);                    \
    }                                                                   \

#define SIMPLE_LOG_MSG(level, message, ...)                             \
{                                                                       \
    if ((level) <= dbgLevel)                                            \
    {                                                                   \
        struct timespec ts;                                             \
        niova_unstable_clock(&ts);                                      \
        fprintf(stderr, "<%ld.%lu:%s:%s:%s@%d> " message "\n",          \
                ts.tv_sec, ts.tv_nsec,                                  \
                ll_to_string(level), thread_name_get(), __func__,       \
                __LINE__, ##__VA_ARGS__);                               \
        if ((level) == LL_FATAL)                                        \
            thread_abort();                                             \
    }                                                                   \
}

#define LOG_MSG(lvl, message, ...)                                      \
{                                                                       \
    REGISTY_ENTRY_FUNCTION_GENERATE;                                    \
                                                                        \
    SIMPLE_LOG_MSG(logEntryInfo.lei_level == LL_ANY ?                   \
                   lvl : logEntryInfo.lei_level, message, ##__VA_ARGS__); \
}

#define FATAL_MSG(message, ...)                         \
    SIMPLE_LOG_MSG(LL_FATAL, message, ##__VA_ARGS__)

#define FATAL_IF(cond, message, ...)            \
{                                               \
    if ((cond))                                 \
    {                                           \
        FATAL_MSG(message, ##__VA_ARGS__);      \
    }                                           \
}

#define NIOVA_ASSERT(cond)                                      \
{                                                               \
    if (!(cond))                                                \
        FATAL_MSG("failed assertion: "#cond);                   \
}

#define NIOVA_ASSERT_strerror(cond)                                     \
{                                                                       \
    if (!(cond))                                                        \
        FATAL_MSG("failed assertion: "#cond", %s", strerror(errno));    \
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

void
log_level_set(enum log_level);

int
log_lreg_cb(enum lreg_node_cb_ops, struct lreg_node *lrn, struct lreg_value *);

init_ctx_t
log_subsys_init(void) __attribute__ ((constructor (LOG_SUBSYS_CTOR_PRIORITY)));

#endif //NIOVA_LOG_H
