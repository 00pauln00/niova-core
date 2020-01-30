/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef REF_TREE_H
#define REF_TREE_H 1

#include "lock.h"
#include "log.h"
#include "tree.h"

#define REF_TREE_PROTOTYPE RB_PROTOTYPE

/**
 * REF_TREE_INIT_ALT_REF - "ALT_REF" means "use an alternate initial ref cnt".
 */
#define REF_TREE_INIT_ALT_REF(rt, constructor_fn, destructor_fn, ref)   \
    {                                                                   \
        spinlock_init(&(rt)->lock);                                     \
        (rt)->initial_ref_cnt = ref;                                    \
        RB_INIT(&(rt)->rt_head);                                        \
        (rt)->constructor = constructor_fn;                             \
        (rt)->destructor = destructor_fn;                               \
    }

#define REF_TREE_INITIAL_REF_CNT(rt) (rt)->initial_ref_cnt

#define REF_TREE_INIT(rt, constructor_fn, destructor_fn)                \
    REF_TREE_INIT_ALT_REF(rt, constructor_fn, destructor_fn, 1);

#define REF_TREE_HEAD(name, type)                               \
    RB_HEAD(_RT_##name, type);                                  \
    struct name                                                 \
    {                                                           \
        struct _RT_##name rt_head;                              \
        unsigned int   initial_ref_cnt;                         \
        spinlock_t     lock;                                    \
        struct type *(*constructor)(const struct type *);       \
        int          (*destructor)(struct type *);              \
    }


#define REF_TREE_MIN(name, head, type, field)         \
    ({                                                \
        struct type *elm = NULL;                      \
        spinlock_lock(&(head)->lock);                 \
        elm = RB_MIN(_RT_##name, &(head)->rt_head);   \
        if (elm)                                      \
        {                                             \
            NIOVA_ASSERT(elm->field.rbe_ref_cnt > 0); \
            elm->field.rbe_ref_cnt++;                 \
        }                                             \
        spinlock_unlock(&(head)->lock);               \
        elm;                                          \
    })

#define REF_TREE_ENTRY(type) REF_RB_ENTRY_PACKED(type)

#define REF_TREE_GENERATE(name, type, field, cmp)                       \
    RB_GENERATE(_RT_##name, type, field, cmp);                          \
                                                                        \
    bool                                                                \
    name##_PUT(struct name *head, struct type *elm)                     \
    {                                                                   \
        bool removed = false;                                           \
        spinlock_lock(&head->lock);                                     \
        elm->field.rbe_ref_cnt--;                                       \
        NIOVA_ASSERT(elm->field.rbe_ref_cnt >= 0);                      \
        if (!elm->field.rbe_ref_cnt)                                    \
        {                                                               \
            RB_REMOVE(_RT_##name, &head->rt_head, elm);                 \
            removed = true;                                             \
        }                                                               \
        spinlock_unlock(&head->lock);                                   \
        if (removed)                                                    \
            head->destructor(elm);                                      \
        return removed;                                                 \
    }                                                                   \
                                                                        \
    static struct type *                                                \
    name##_LOOKUP_LOCKED(struct name *head,                             \
                         const struct type *lookup_elm)                 \
    {                                                                   \
        struct type *elm = RB_FIND(_RT_##name, &head->rt_head,          \
                                   (struct type *)lookup_elm);          \
        if (elm)                                                        \
        {                                                               \
            NIOVA_ASSERT(elm->field.rbe_ref_cnt > 0);                   \
            elm->field.rbe_ref_cnt++;                                   \
        }                                                               \
        return elm;                                                     \
    }                                                                   \
                                                                        \
    struct type *                                                       \
    name##_GET(struct name *head, const struct type *lookup_elm,        \
               const bool add, int *ret)                                \
    {                                                                   \
        if (ret)                                                        \
            *ret = 0;                                                   \
                                                                        \
        spinlock_lock(&head->lock);                                     \
        struct type *elm = name##_LOOKUP_LOCKED(head, lookup_elm);      \
        spinlock_unlock(&head->lock);                                   \
                                                                        \
        if (elm || !add)                                                \
        {                                                               \
            if (add && ret)                                             \
                *ret = -EEXIST;                                         \
                                                                        \
            return elm;                                                 \
        }                                                               \
                                                                        \
        elm = head->constructor(lookup_elm);                            \
        if (!elm)                                                       \
        {                                                               \
            if (ret)                                                    \
                *ret = -ENOMEM;                                         \
                                                                        \
            return NULL;                                                \
        }                                                               \
                                                                        \
        spinlock_lock(&head->lock);                                     \
        elm->field.rbe_ref_cnt = head->initial_ref_cnt;                 \
        struct type *already = RB_INSERT(_RT_##name, &head->rt_head,    \
                                         elm);                          \
        spinlock_unlock(&head->lock);                                   \
                                                                        \
        if (already)                                                    \
        {                                                               \
            (int)head->destructor(elm);                                 \
            elm = already;                                              \
            if (ret)                                                    \
                *ret = -EALREADY;                                       \
        }                                                               \
                                                                        \
        return elm;                                                     \
    }                                                                   \

#define RT_GET(name, head, lookup_elm, add, ret) \
    name##_GET(head, lookup_elm, add, ret)

#define RT_LOOKUP(name, head, lookup_elm)       \
    name##_GET(head, lookup_elm, false, NULL)

#define RT_GET_ADD(name, head, lookup_elm, ret)  \
    name##_GET(head, lookup_elm, true, ret)

#define RT_PUT(name, head, elm)                 \
    name##_PUT(head, elm)

#endif //REF_TREE_H
