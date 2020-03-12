/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef _REGISTRY_H
#define _REGISTRY_H 1

#include "init.h"
#include "ctor.h"
#include "queue.h"
#include "atomic.h"

/* "Install context" is designated for threads other than the glreg service
 * thread which may add new registry objects.
 */
typedef void lreg_install_ctx_t;
typedef int  lreg_install_int_ctx_t;
typedef bool lreg_install_bool_ctx_t;
typedef	void lreg_svc_ctx_t;
typedef void * lreg_svc_thread_t;
typedef	struct lreg_node * lreg_svc_lrn_ctx_t;
typedef	int  lreg_svc_int_ctx_t;
typedef	void lreg_user_ctx_t;
typedef	int lreg_user_int_ctx_t;

typedef bool (*lrn_walk_cb_t)(struct lreg_node *, void *, const int);

struct lreg_value;
typedef void (*lrn_recurse_cb_t)(struct lreg_value *, const int, const int,
                                 const bool);

#define LREG_VALUE_STRING_MAX 255

enum lreg_value_types
{
    LREG_VAL_TYPE_NONE,
    LREG_VAL_TYPE_ARRAY,
    LREG_VAL_TYPE_BOOL,
    LREG_VAL_TYPE_OBJECT,
    LREG_VAL_TYPE_ANON_OBJECT,
    LREG_VAL_TYPE_SIGNED_VAL,
    LREG_VAL_TYPE_STRING,
    LREG_VAL_TYPE_UNSIGNED_VAL,
    LREG_VAL_TYPE_FLOAT_VAL,
    LREG_VAL_TYPE_ANY,
};

enum lreg_user_types
{
    LREG_USER_TYPE_NONE = 0,
    LREG_USER_TYPE_FAULT,
    LREG_USER_TYPE_LOG_file,
    LREG_USER_TYPE_LOG_func,
    LREG_USER_TYPE_NIOSD_IO,
    LREG_USER_TYPE_NIOSD_IO_CTX,
    LREG_USER_TYPE_NIOSD_IO_STATS,
    LREG_USER_TYPE_HISTOGRAM,
    LREG_USER_TYPE_ROOT,
    LREG_USER_TYPE_RAFT,
    LREG_USER_TYPE_RAFT_PEER_STATS,
    LREG_USER_TYPE_ANY,
};

enum lreg_node_cb_ops
{
    LREG_NODE_CB_OP_GET_NODE_INFO,
    LREG_NODE_CB_OP_WRITE_VAL,
    LREG_NODE_CB_OP_READ_VAL,
    LREG_NODE_CB_OP_INSTALL_NODE,
    LREG_NODE_CB_OP_DESTROY_NODE,
};

#define LREG_NODE_CB_OP_GET_NAME LREG_NODE_CB_OP_GET_NODE_INFO

struct lreg_value_data
{
    union
    {
        uint64_t lrv_unsigned_val;
        int64_t  lrv_signed_val;
        float    lrv_float;
        bool     lrv_bool;
        char     lrv_string[LREG_VALUE_STRING_MAX + 1];
    };
};

/**
 * -- struct lreg_value --
 * Complex value structure used for obtained multi-faceted object values from
 * local registry nodes.
 * - GET operation:
 * @lrv_value_idx_in:  logical value index for a value "get" operation.
 * @lrv_op_in:  the op from which the results were produced.
 * @lrv_num_keys_out:  number of values found at this object.
 * @lrv_node_type_out:  value type corresponding to the requested index.
 * @lrv_value_out:  storage for output value.
 * - PUT operation:
 * @lrv_key_string:  key string for the provided value.
 * @lrv_value_in:  input value storage.
 */
struct lreg_value
{
    char                            lrv_key_string[LREG_VALUE_STRING_MAX + 1];
    unsigned int                    lrv_value_idx_in;
    enum lreg_node_cb_ops           lrv_op_in;
    union
    {
        struct
        {
            unsigned int           lrv_num_keys_out;
            enum lreg_value_types  lrv_value_type_out;
            enum lreg_user_types   lrv_user_type_out;
            struct lreg_value_data lrv_value_out;
        } get;

        struct
        {
            struct lreg_value_data lrv_value_in;
        } put;
    };
};

#define lrv_node_type_out lrv_value_type_out //xxx to be removed

#define LREG_VALUE_TO_OUT_STR(lrv)              \
    (lrv)->get.lrv_value_out.lrv_string

#define LREG_VALUE_TO_KEY_STR(lrv)              \
    (lrv)->lrv_key_string

#define LREG_VALUE_TO_BOOL(lrv)                 \
    (lrv)->get.lrv_value_out.lrv_bool

#define LREG_VALUE_TO_IN_STR(lrv)               \
    (lrv)->put.lrv_value_in.lrv_string

#define LREG_VALUE_TO_OUT_SIGNED_INT(lrv)       \
    (lrv)->get.lrv_value_out.lrv_signed_val

#define LREG_VALUE_TO_OUT_UNSIGNED_INT(lrv)     \
    (lrv)->get.lrv_value_out.lrv_unsigned_val

#define LREG_VALUE_TO_OUT_FLOAT(lrv)            \
    (lrv)->get.lrv_value_out.lrv_float

#define LREG_VALUE_TO_REQ_TYPE(lrv)             \
    (lrv)->get.lrv_value_type_out

#define LREG_VALUE_TO_USER_TYPE(lrv)             \
    (lrv)->get.lrv_user_type_out

struct lreg_node;

CIRCLEQ_HEAD(lreg_node_list, lreg_node);

typedef int (*lrn_cb_t)(enum lreg_node_cb_ops, struct lreg_node *,
                        struct lreg_value *);

/**
 * -- struct lreg_node --
 */
struct lreg_node
{
    enum lreg_user_types      lrn_user_type;
    uint8_t                   lrn_install_state;
    uint8_t                   lrn_tmp_node:1,
                              lrn_statically_allocated:1,
                              lrn_root_node:1,
                              lrn_monitor:1,
                              lrn_may_destroy:1,
                              lrn_array_element:1;
    void                     *lrn_cb_arg;
    lrn_cb_t                  lrn_cb;
    CIRCLEQ_ENTRY(lreg_node)  lrn_lentry;
    union
    {
        struct lreg_node_list lrn_head; //arrays and objects
        struct lreg_node     *lrn_parent_for_install_only;
    };
};

static inline char
lreg_val_to_type(enum lreg_value_types type)
{
    switch (type)
    {
    case LREG_VAL_TYPE_NONE:
        return 'n';
    case LREG_VAL_TYPE_ARRAY:
        return 'A';
    case LREG_VAL_TYPE_BOOL:
        return 'B';
    case LREG_VAL_TYPE_OBJECT:
        return 'O';
    case LREG_VAL_TYPE_ANON_OBJECT:
        return 'o';
    case LREG_VAL_TYPE_SIGNED_VAL:
        return 'S';
    case LREG_VAL_TYPE_UNSIGNED_VAL:
        return 'U';
    default:
        break;
    }
    return 'a';
}

static inline char
lreg_node_to_user_type(const struct lreg_node *lrn)
{
    switch (lrn->lrn_user_type)
    {
    case LREG_USER_TYPE_NONE:
        return 'n';
    case LREG_USER_TYPE_FAULT:
        return 'f';
    case LREG_USER_TYPE_LOG_file:
        return 'L';
    case LREG_USER_TYPE_LOG_func:
        return 'l';
    case LREG_USER_TYPE_ROOT:
        return 'R';
    default:
        break;
    }
    return 'a';
}

#define LREG_NODE_NOT_INSTALLED 0
#define LREG_NODE_INSTALLING    1
#define LREG_NODE_INSTALLED     2

static inline char
lreg_node_to_install_state(const struct lreg_node *lrn)
{
    switch (lrn->lrn_install_state)
    {
    case LREG_NODE_NOT_INSTALLED:
        return '-';
    case LREG_NODE_INSTALLING:
        return 'i';
    case LREG_NODE_INSTALLED:
        return 'I';
    default:
        break;
    }
    return '?';
}

#define DBG_LREG_NODE(log_level, lrn, fmt, ...)                         \
{                                                                       \
    struct lreg_value lrv;                                              \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "lrn@%p %s %c%c%c%c%c%c%c%c arg=%p "fmt,             \
                   (lrn),                                               \
                   (const char *)({                                     \
                           (lrn)->lrn_cb(LREG_NODE_CB_OP_GET_NAME,      \
                                         (lrn), &lrv);                  \
                           LREG_VALUE_TO_OUT_STR(&lrv);                 \
                       }),                                              \
                   lreg_node_to_user_type(lrn),                         \
                   lreg_node_to_install_state(lrn),                     \
                   (lrn)->lrn_statically_allocated  ? 's' : '-',        \
                   (lrn)->lrn_tmp_node              ? 't' : '-',        \
                   (lrn)->lrn_root_node             ? 'r' : '-',        \
                   (lrn)->lrn_may_destroy           ? 'd' : '-',        \
                   (lrn)->lrn_monitor               ? 'm' : '-',        \
                   (lrn)->lrn_array_element         ? 'a' : '-',        \
                   (lrn)->lrn_cb_arg, ##__VA_ARGS__);                   \
}

static inline bool
lreg_statically_allocated_node_check(const struct lreg_node *lrn)
{
    return (lrn->lrn_statically_allocated && !lrn->lrn_may_destroy) ?
        true : false;
}

static inline bool
lreg_node_needs_installation(const struct lreg_node *lrn)
{
    return lrn->lrn_install_state == LREG_NODE_NOT_INSTALLED ? true : false;
}

static inline bool
lreg_node_install_prep_ok(struct lreg_node *lrn)
{
    return niova_atomic_cas(&lrn->lrn_install_state, LREG_NODE_NOT_INSTALLED,
                            LREG_NODE_INSTALLING) ? true : false;
}

static inline bool
lreg_node_install_complete(struct lreg_node *lrn)
{
    return niova_atomic_cas(&lrn->lrn_install_state, LREG_NODE_INSTALLING,
                            LREG_NODE_INSTALLED) ? true : false;
}

static inline int
lreg_node_exec_lrn_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                      struct lreg_value *lrv)
{
    if (!lrn)
        return -EINVAL;

    if (lrv)
        lrv->lrv_op_in = op;

    return lrn->lrn_cb(op, lrn, lrv);
}

struct lreg_node *
lreg_root_node_get(void);

lreg_install_int_ctx_t
lreg_node_install_prepare(struct lreg_node *, struct lreg_node *);

void
lreg_node_init(struct lreg_node *, enum lreg_user_types, lrn_cb_t, void *,
               bool);

lreg_install_ctx_t
lreg_node_object_init(struct lreg_node *, enum lreg_user_types, bool);

init_ctx_t
lreg_subsystem_init(void)
    __attribute__ ((constructor (LREG_SUBSYS_CTOR_PRIORITY)));

destroy_ctx_t
lreg_subsystem_destroy(void)
    __attribute__ ((destructor (LREG_SUBSYS_CTOR_PRIORITY)));

#define LREG_ROOT_ENTRY_GENERATE(name, user_type)                       \
    static lreg_install_int_ctx_t                                       \
    lreg_root_cb##name(enum lreg_node_cb_ops op, struct lreg_node *lrn, \
                       struct lreg_value *lreg_val)                     \
    {                                                                   \
        switch (op)                                                     \
        {                                                               \
        case LREG_NODE_CB_OP_GET_NODE_INFO:                             \
            if (!lreg_val)                                              \
                return -EINVAL;                                         \
                                                                        \
            lreg_val->get.lrv_num_keys_out = 1;                         \
            snprintf(lreg_val->lrv_key_string,                          \
                     LREG_VALUE_STRING_MAX, #name);                     \
            break;                                                      \
        case LREG_NODE_CB_OP_READ_VAL:     /* fall through */           \
            if (!lreg_val)                                              \
                return -EINVAL;                                         \
            if (lreg_val->lrv_value_idx_in != 0)                        \
                return -EINVAL;                                         \
            lreg_val->get.lrv_value_type_out = LREG_VAL_TYPE_ARRAY;     \
            lreg_val->get.lrv_user_type_out = user_type;                \
            snprintf(lreg_val->lrv_key_string,                          \
                     LREG_VALUE_STRING_MAX, #name);                     \
            break;                                                      \
        case LREG_NODE_CB_OP_WRITE_VAL:    /* fall through */           \
            return -EOPNOTSUPP;                                         \
        case LREG_NODE_CB_OP_INSTALL_NODE: /* fall through */           \
        case LREG_NODE_CB_OP_DESTROY_NODE: /* fall through */           \
            break;                                                      \
        default:                                                        \
            return -ENOENT;                                             \
        }                                                               \
                                                                        \
        return 0;                                                       \
    }                                                                   \
                                                                        \
    struct lreg_node rootEntry##name = {                                \
        .lrn_cb_arg = (void *)1,                                        \
        .lrn_user_type = user_type,                                     \
        .lrn_statically_allocated = 1,                                  \
        .lrn_cb = lreg_root_cb##name                                    \
    }                                                                   \

#define LREG_ROOT_ENTRY_EXPORT(name)                                    \
    extern struct lreg_node rootEntry##name

#define LREG_ROOT_ENTRY_PTR(name)                                       \
    &rootEntry##name

#define LREG_ROOT_ENTRY_INSTALL(name)                                   \
    NIOVA_ASSERT(!lreg_node_install_prepare(LREG_ROOT_ENTRY_PTR(name),  \
                                            lreg_root_node_get()))

lreg_user_int_ctx_t
lreg_node_recurse(const char *);
//lreg_node_recurse(const char *, lrn_recurse_cb_t);

void
lreg_node_walk(const struct lreg_node *parent, lrn_walk_cb_t lrn_wcb,
               void *cb_arg, const int depth,
               const enum lreg_user_types user_type);

#endif //_REGISTRY_H
