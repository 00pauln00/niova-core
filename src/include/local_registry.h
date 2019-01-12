/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef LOCAL_REGISTRY_H
#define LOCAL_REGISTRY_H 1

#include "queue.h"
#include "common.h"
#include "atomic.h"

/* "Install context" is designated for threads other than the glreg service
 * thread which may add new registry objects.
 */
typedef void lreg_install_ctx_t;
typedef int  lreg_install_int_ctx_t;
typedef bool lreg_install_bool_ctx_t;
typedef	void lreg_svc_ctx_t;

#define LREG_NODE_NAME_MAX 63
#define LREG_VALUE_STRING_MAX 63

enum lreg_node_types
{
    LREG_NODE_TYPE_NONE = 0,
    LREG_NODE_TYPE_ARRAY,
    LREG_NODE_TYPE_BOOL,
    LREG_NODE_TYPE_OBJECT,
    LREG_NODE_TYPE_SIGNED_VAL,
    LREG_NODE_TYPE_STRING,
    LREG_NODE_TYPE_UNSIGNED_VAL,
    LREG_NODE_TYPE_ANY,
};

enum lreg_user_types
{
    LREG_USER_TYPE_NONE = 0,
    LREG_USER_TYPE_FAULT,
    LREG_USER_TYPE_LOG,
    LREG_USER_TYPE_ROOT,
    LREG_USER_TYPE_ANY,
};

enum lreg_node_cb_ops
{
    LREG_NODE_CB_OP_GET_NAME,
    LREG_NODE_CB_OP_WRITE_VAL,
    LREG_NODE_CB_OP_READ_VAL,
    LREG_NODE_CB_OP_INSTALL_NODE,
    LREG_NODE_CB_OP_DESTROY_NODE,
};

/**
 * -- struct lreg_value --
 */
struct lreg_value
{
    union
    {
        uint64_t lrv_unsigned_val;
        uint64_t lrv_signed_val;
        float    lrv_float;
        bool     lrv_bool;
        char     lrv_string[LREG_VALUE_STRING_MAX];
    };
};

struct lreg_node;

typedef int (*lrn_cb_t)(enum lreg_node_cb_ops, struct lreg_node *,
                        struct lreg_value *);

/**
 * -- struct lreg_node --
 */
struct lreg_node
{
//    char                             lrn_name[LREG_NODE_NAME_MAX + 1];
    enum lreg_node_types             lrn_node_type;
    enum lreg_user_types             lrn_user_type;
    uint8_t                          lrn_install_state;
    uint16_t                         lrn_tmp_node:1,
                                     lrn_statically_allocated:1,
                                     lrn_root_node:1,
                                     lrn_may_destroy:1;
    void                            *lrn_cb_arg;
    lrn_cb_t                         lrn_cb;
    SLIST_ENTRY(lreg_node)           lrn_lentry;
    SLIST_HEAD(lreg_list, lreg_node) lrn_head; //arrays and objects
};

static inline char
lreg_node_to_node_type(const struct lreg_node *lrn)
{
    switch (lrn->lrn_node_type)
    {
    case LREG_NODE_TYPE_NONE:
        return 'n';
    case LREG_NODE_TYPE_ARRAY:
        return 'A';
    case LREG_NODE_TYPE_BOOL:
        return 'B';
    case LREG_NODE_TYPE_OBJECT:
        return 'O';
    case LREG_NODE_TYPE_SIGNED_VAL:
        return 'S';
    case LREG_NODE_TYPE_UNSIGNED_VAL:
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
    case LREG_USER_TYPE_LOG:
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
    log_msg(log_level, "lrn@%p %s %c%c%c%c%c%c%c arg=%p "fmt,           \
            (lrn),                                                      \
            (const char *)({                                            \
                struct lreg_value lrv;                                  \
                (lrn)->lrn_cb(LREG_NODE_CB_OP_GET_NAME, (lrn), &lrv);   \
                lrv.lrv_string;                                         \
            }),                                                         \
            lreg_node_to_node_type(lrn),                                \
            lreg_node_to_user_type(lrn),                                \
            lreg_node_to_install_state(lrn),                            \
            (lrn)->lrn_statically_allocated  ? 's' : '-',               \
            (lrn)->lrn_tmp_node              ? 't' : '-',               \
            (lrn)->lrn_root_node             ? 'r' : '-',               \
            (lrn)->lrn_may_destroy           ? 'd' : '-',               \
            (lrn)->lrn_cb_arg, ##__VA_ARGS__);                          \
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

lreg_install_int_ctx_t
lreg_node_install(struct lreg_node *, struct lreg_node *);

lreg_install_int_ctx_t
lreg_node_install_in_root(struct lreg_node *);

void
lreg_node_init(struct lreg_node *, enum lreg_node_types, enum lreg_user_types,
               lrn_cb_t, bool);

lreg_install_ctx_t
lreg_node_object_init(struct lreg_node *, enum lreg_user_types, bool);

void
lreg_subsystem_init(void);

#endif //LOCAL_REGISTRY_H
