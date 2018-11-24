/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_LOCK_H
#define NIOVA_LOCK_H 1

#include "log.h"
#include <pthread.h>
#include <errno.h>

typedef pthread_spinlock_t spinlock_t;

static inline void
spinlock_init(spinlock_t *lock)
{
    int rc = pthread_spin_init(lock, 0);
    NIOVA_ASSERT(!rc);
}

static inline void
spinlock_destroy(spinlock_t *lock)
{
    int rc = pthread_spin_destroy(lock);
    NIOVA_ASSERT(!rc);
}

static inline void
spinlock_lock(spinlock_t *lock)
{
    int rc = pthread_spin_lock(lock);
    NIOVA_ASSERT(!rc);
}

static inline int
spinlock_trylock(spinlock_t *lock)
{
    int rc = pthread_spin_trylock(lock);
    NIOVA_ASSERT(!rc || rc ==  EBUSY);

    return rc;
}

static inline void
spinlock_unlock(spinlock_t *lock)
{
    int rc = pthread_spin_unlock(lock );
    NIOVA_ASSERT(!rc);
}


#endif //NIOVA_LOCK_H
