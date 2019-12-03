/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_LOG_H
#define NIOVA_LOG_H 1

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#include <stdio.h>
#include <pthread.h>
#undef _GNU_SOURCE
#endif

#include "common.h"

#include "init.h"
#include "thread.h"
#include "util.h"
#include "registry.h"

LREG_ROOT_ENTRY_EXPORT(log_entry_map);

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
    case LL_NOTIFY:
        return "notify";
    case LL_DEBUG:
        return "debug";
    case LL_TRACE:
        return "trace";
    default:
        break;
    }

    return "default";
}

struct log_entry_info
{
    enum log_level  lei_level;
    int             lei_lineno;
    size_t          lei_exec_cnt;
    union
    {
        const char *lei_func;
        const char *lei_file;
    };
};

#define REGISTRY_ENTRY_FILE_GENERATE                                    \
    static struct log_entry_info logEntryFileInfo = {                   \
        .lei_level = LL_ANY,                                            \
        .lei_file = __FILE__,                                           \
    };                                                                  \
                                                                        \
    static struct lreg_node regFileEntry = {                            \
        .lrn_cb_arg = &logEntryFileInfo,                                \
        .lrn_node_type = LREG_NODE_TYPE_ANON_OBJECT,                    \
        .lrn_user_type = LREG_USER_TYPE_LOG_file,                       \
        .lrn_statically_allocated = 1,                                  \
        .lrn_array_element = 1,                                         \
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
        .lrn_node_type = LREG_NODE_TYPE_ANON_OBJECT,                    \
        .lrn_user_type = LREG_USER_TYPE_LOG_func,                       \
        .lrn_statically_allocated = 1,                                  \
        .lrn_array_element = 1,                                         \
        .lrn_cb = log_lreg_cb,                                          \
    };                                                                  \
    int _node_install_rc = 0;                                           \
    if (lreg_node_needs_installation(&regFileEntry))                    \
    {                                                                   \
        _node_install_rc =                                              \
            lreg_node_install_prepare(&regFileEntry,                    \
                                      LREG_ROOT_ENTRY_PTR(log_entry_map)); \
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

#define FUNC_ENTRY(level)                       \
    LOG_MSG(level, "enter")

#define FUNC_EXIT(level)                        \
    LOG_MSG(level, "exit")

#define SIMPLE_FUNC_ENTRY(level)                \
    SIMPLE_LOG_MSG(level, "enter")

#define SIMPLE_FUNC_EXIT(level)                 \
    SIMPLE_LOG_MSG(level, "exit")

/**
 * LOG_MSG - during normal runtime, take the log level first from this
 *    registry entry's level, and next from the file's level.  If neither are
 *    set, then use the level provided by the caller.
 */
#define LOG_MSG(lvl, message, ...)                                      \
{                                                                       \
    if (!init_ctx())                                                    \
    {                                                                   \
        REGISTY_ENTRY_FUNCTION_GENERATE;                                \
                                                                        \
        logEntryInfo.lei_exec_cnt++;                                    \
                                                                        \
        enum log_level my_lvl = logEntryInfo.lei_level != LL_ANY ?      \
            logEntryInfo.lei_level : logEntryFileInfo.lei_level;        \
                                                                        \
        SIMPLE_LOG_MSG(my_lvl == LL_ANY ? lvl : logEntryInfo.lei_level, \
                       message, ##__VA_ARGS__);                         \
    }                                                                   \
    else                                                                \
    {                                                                   \
        SIMPLE_LOG_MSG(lvl, message, ##__VA_ARGS__);                    \
    }                                                                   \
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

#define FATAL_IF_strerror(cond, message, ...)           \
{                                                       \
    if ((cond))                                         \
    {                                                   \
        FATAL_MSG(message": %s", ##__VA_ARGS__, strerror(errno));        \
    }                                                   \
}

#define EXIT_ERROR_MSG(exit_val, message, ...)          \
{                                                       \
    SIMPLE_LOG_MSG(LL_ERROR, message, ##__VA_ARGS__);   \
    exit(exit_val);                                     \
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

#define STDERR_MSG(message, ...)                                      \
{                                                                     \
    fprintf(stderr, "<%lx:%s@%d> " message "\n",                      \
            thread_id_get(), __func__,                                \
            __LINE__,##__VA_ARGS__);                                  \
}

static inline bool
log_level_is_valid(unsigned int level)
{
    return level < LL_MAX ? true : false;
}

void
log_level_set(enum log_level);

void
log_level_set_from_env(void);

int
log_lreg_cb(enum lreg_node_cb_ops, struct lreg_node *lrn, struct lreg_value *);

init_ctx_t
log_subsys_init(void) __attribute__ ((constructor (LOG_SUBSYS_CTOR_PRIORITY)));

destroy_ctx_t
log_subsys_destroy(void)
    __attribute__ ((destructor (LOG_SUBSYS_CTOR_PRIORITY)));

#endif //NIOVA_LOG_H
