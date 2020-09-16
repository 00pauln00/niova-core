/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef REF_TREE_H
#define REF_TREE_H 1

#include <pthread.h>

#include "log.h"
#include "tree.h"

#define REF_TREE_PROTOTYPE RB_PROTOTYPE

/**
 * REF_TREE_INIT_ALT_REF - "ALT_REF" means "use an alternate initial ref cnt".
 */
#define REF_TREE_INIT_ALT_REF(rt, constructor_fn, destructor_fn, ref) \
    {                                                                 \
        pthread_mutex_init(&(rt)->mutex, NULL);                       \
        (rt)->initial_ref_cnt = ref;                                  \
        RB_INIT(&(rt)->rt_head);                                      \
        (rt)->constructor = constructor_fn;                           \
        (rt)->destructor = destructor_fn;                             \
    }

#define REF_TREE_INITIAL_REF_CNT(rt) (rt)->initial_ref_cnt

#define REF_TREE_INIT(rt, constructor_fn, destructor_fn) \
    REF_TREE_INIT_ALT_REF(rt, constructor_fn, destructor_fn, 1);

#define REF_TREE_DESTROY(rt)                 \
    {                                        \
        pthread_mutex_destroy(&(rt)->mutex); \
    }

#define REF_TREE_HEAD(name, type)                          \
    RB_HEAD(_RT_##name, type);                             \
    struct name                                            \
    {                                                      \
        struct _RT_##name rt_head;                         \
        unsigned int    initial_ref_cnt;                   \
        pthread_mutex_t mutex;                             \
        struct type  *(*constructor)(const struct type *); \
        int           (*destructor)(struct type *);        \
    }

#define REF_TREE_REF_GET_ELEM_LOCKED(elm, field)    \
    do {                                            \
        NIOVA_ASSERT((elm)->field.rte_ref_cnt > 0); \
        (elm)->field.rte_ref_cnt++;                 \
    } while (0)

#define REF_TREE_REF_PUT_ELEM_LOCKED(elm, field)    \
    do {                                            \
        NIOVA_ASSERT((elm)->field.rte_ref_cnt > 0); \
        (elm)->field.rte_ref_cnt--;                 \
    } while (0)

#define REF_TREE_MIN(name, head, type, field)         \
    ({                                                \
        struct type *elm = NULL;                      \
        pthread_mutex_lock(&(head)->mutex);           \
        elm = RB_MIN(_RT_##name, &(head)->rt_head);   \
        if (elm)                                      \
            REF_TREE_REF_GET_ELEM_LOCKED(elm, field); \
        pthread_mutex_unlock(&(head)->mutex);         \
        elm;                                          \
    })

#define REF_TREE_ENTRY(type)             \
struct {                                 \
    RB_ENTRY_PACKED(type) RTE_RBE;       \
    int rte_ref_cnt;                     \
}

#define REF_TREE_GENERATE(name, type, field, cmp)                    \
    RB_GENERATE(_RT_##name, type, field.RTE_RBE, cmp);               \
                                                                     \
    bool                                                             \
    name##_PUT(struct name *head, struct type *elm)                  \
    {                                                                \
        bool removed = false;                                        \
        pthread_mutex_lock(&head->mutex);                            \
        REF_TREE_REF_PUT_ELEM_LOCKED(elm, field);                    \
        if (!elm->field.rte_ref_cnt)                                 \
        {                                                            \
            struct type *old =                                       \
                RB_REMOVE(_RT_##name, &head->rt_head, elm);          \
            removed = true;                                          \
            NIOVA_ASSERT(elm == old);                                \
        }                                                            \
        pthread_mutex_unlock(&head->mutex);                          \
        if (removed)                                                 \
            head->destructor(elm);                                   \
        return removed;                                              \
    }                                                                \
                                                                     \
    static struct type *                                             \
    name##_LOOKUP_LOCKED(struct name *head,                          \
                         const struct type *lookup_elm)              \
    {                                                                \
        struct type *elm = RB_FIND(_RT_##name, &head->rt_head,       \
                                   (struct type *)lookup_elm);       \
        if (elm)                                                     \
            REF_TREE_REF_GET_ELEM_LOCKED(elm, field);                \
                                                                     \
        return elm;                                                  \
    }                                                                \
                                                                     \
    struct type *                                                    \
    name##_GET(struct name *head, const struct type *lookup_elm,     \
               const bool add, int *ret)                             \
    {                                                                \
        if (ret)                                                     \
            *ret = 0;                                                \
                                                                     \
        pthread_mutex_lock(&head->mutex);                            \
        struct type *elm = name##_LOOKUP_LOCKED(head, lookup_elm);   \
        pthread_mutex_unlock(&head->mutex);                          \
                                                                     \
        if (elm || !add)                                             \
        {                                                            \
            if (add && ret)                                          \
                *ret = -EEXIST;                                      \
                                                                     \
            return elm;                                              \
        }                                                            \
                                                                     \
        elm = head->constructor(lookup_elm);                         \
        if (!elm)                                                    \
        {                                                            \
            if (ret)                                                 \
                *ret = -ENOMEM;                                      \
                                                                     \
            return NULL;                                             \
        }                                                            \
                                                                     \
        pthread_mutex_lock(&head->mutex);                            \
                                                                     \
        struct type *already = RB_INSERT(_RT_##name, &head->rt_head, \
                                         elm);                       \
        if (already)                                                 \
            REF_TREE_REF_GET_ELEM_LOCKED(already, field);            \
        else                                                         \
            elm->field.rte_ref_cnt = head->initial_ref_cnt;          \
                                                                     \
        pthread_mutex_unlock(&head->mutex);                          \
                                                                     \
        if (already)                                                 \
        {                                                            \
            (int)head->destructor(elm);                              \
            elm = already;                                           \
            if (ret)                                                 \
                *ret = -EALREADY;                                    \
        }                                                            \
                                                                     \
        return elm;                                                  \
    }                                                                \

#define RT_GET(name, head, lookup_elm, add, ret) \
    name##_GET(head, lookup_elm, add, ret)

#define RT_LOOKUP(name, head, lookup_elm) \
    name##_GET(head, lookup_elm, false, NULL)

#define RT_GET_ADD(name, head, lookup_elm, ret) \
    name##_GET(head, lookup_elm, true, ret)

#define RT_PUT(name, head, elm)                 \
    name##_PUT(head, elm)

#define RT_FOREACH_LOCKED(x, name, head) \
    RB_FOREACH(x, _RT_##name, &(head)->rt_head)

#define RT_FOREACH_SAFE_LOCKED(x, name, head, y) \
    RB_FOREACH(x, _RT_##name, &(head)->rt_head)

#define RT_FOREACH_REVERSE_LOCKED(x, name, head) \
    RB_FOREACH_REVERSE(x, _RT_##name, &(head)->rt_head)

#define RT_FOREACH_REVERSE_SAFE_LOCKED(x, name, head, y) \
    RB_FOREACH_REVERSE(x, _RT_##name, &(head)->rt_head, y)

#endif //REF_TREE_H
