/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

// https://gcc.gnu.org/onlinedocs/gcc-4.1.0/gcc/Atomic-Builtins.html

#ifndef NIOVA_ATOMIC_H
#define NIOVA_ATOMIC_H 1

#include "common.h"

typedef volatile long long int niova_atomic64_t;
typedef volatile int           niova_atomic32_t;
typedef volatile short int     niova_atomic16_t;
typedef volatile signed char   niova_atomic8_t;

// bool __sync_bool_compare_and_swap (type *ptr, type oldval type newval, ...)
#define niova_atomic_cas __sync_bool_compare_and_swap

// type __sync_xor_and_fetch (type *ptr, type value, ...)
#define niova_atomic_xor __sync_xor_and_fetch

// type __sync_and_and_fetch (type *ptr, type value, ...)
#define niova_atomic_and __sync_xor_and_fetch

#define niova_atomic_inc(ptr)      __sync_add_and_fetch(ptr, 1)
#define niova_atomic_dec(ptr)      __sync_sub_and_fetch(ptr, 1)
#define niova_atomic_add(ptr, val) __sync_add_and_fetch(ptr, val)
#define niova_atomic_sub(ptr, val) __sync_sub_and_fetch(ptr, val)
#define niova_atomic_read(ptr)     *(volatile typeof(* ptr) *)ptr
#define niova_atomic_init(ptr, val) (*(ptr) = (val))

#endif //NIOVA_ATOMIC_H
