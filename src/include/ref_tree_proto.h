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

//XXX I think 'typeof' can be used here!
//https://gcc.gnu.org/onlinedocs/gcc-4.1.0/gcc/Typeof.html#Typeof

#define REF_TREE_PROTOTYPE RB_PROTOTYPE

#define REF_TREE_INIT(rt, constructor_fn, destructor_fn)        \
    {                                                           \
        spinlock_init(&(rt)->lock);                             \
        RB_INIT(&(rt)->rt_head);                                \
        (rt)->constructor = constructor_fn;                     \
        (rt)->destructor = destructor_fn;                       \
    }

#define REF_TREE_HEAD(name, type)                               \
    RB_HEAD(_RT_##name, type);                                  \
    struct name                                                 \
    {                                                           \
        struct _RT_##name rt_head;                              \
        spinlock_t     lock;                                    \
        struct type *(*constructor)(const struct type *);       \
        int          (*destructor)(struct type *);              \
    }

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
               const bool add)                                          \
    {                                                                   \
        spinlock_lock(&head->lock);                                     \
        struct type *elm = name##_LOOKUP_LOCKED(head, lookup_elm);      \
        spinlock_unlock(&head->lock);                                   \
                                                                        \
        if (elm || !add)                                                \
            return elm;                                                 \
                                                                        \
        elm = head->constructor(lookup_elm);                            \
        if (!elm)                                                       \
            return NULL;                                                \
                                                                        \
        spinlock_lock(&head->lock);                                     \
        elm->field.rbe_ref_cnt = 1;                                     \
        struct type *already = RB_INSERT(_RT_##name, &head->rt_head,    \
                                         elm);                          \
        spinlock_unlock(&head->lock);                                   \
                                                                        \
        if (already)                                                    \
        {                                                               \
            (int)head->destructor(elm);                                 \
            elm = already;                                              \
        }                                                               \
        return elm;                                                     \
    }                                                                   \

#define RT_GET(name, head, lookup_elm)     name##_GET(head, lookup_elm, false);
#define RT_GET_ADD(name, head, lookup_elm) name##_GET(head, lookup_elm, true);
#define RT_PUT(name, head, elm)            name##_PUT(head, elm);

#endif //REF_TREE_H
