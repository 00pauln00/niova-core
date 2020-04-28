/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2020
 */
#ifndef NIOVA_FAULT_INJECT_H
#define NIOVA_FAULT_INJECT_H 1

#include "common.h"

#include "atomic.h"
#include "registry.h"
#include "util.h"

enum fault_inject_entries
{
    FAULT_INJECT_any,
    FAULT_INJECT_disabled,
    FAULT_INJECT__MAX,
    FAULT_INJECT__MIN = FAULT_INJECT_any,
} PACKED;

#if 0
enum fault_inject_how
{
    FAULT_INJECT_TYPE_system, // system decides when to inject
    FAULT_INJECT_TYPE_user    // fault is activated only on user instruction
} PACKED;
#endif

enum fault_inject_when
{
    FAULT_INJECT_PERIOD_one_time_only,
    FAULT_INJECT_PERIOD_every_time,
    FAULT_INJECT_PERIOD_per_freq_in_seconds,
    FAULT_INJECT_PERIOD_fixed_number_of_times,
} PACKED;


struct fault_injection
{
    const char            *flti_name;
    const char            *flti_file;
    const char            *flti_func;
    const uint32_t         flti_lineno;
//  enum fault_inject_how  flti_how;
    enum fault_inject_when flti_when;
    uint16_t               flti_freq_seconds;
    uint32_t               flti_num_remaining;
    uint32_t               flti_inject_cnt;
    time_t                 flti_last;
    uint64_t               flti_cond_exec_cnt:62,
                           flti_enabled:1;
    struct lreg_node       flti_lrn;
};

struct fault_injection_stub
{
    struct fault_injection   *fis_flti;
    niova_atomic8_t           fis_atomic;
};

static inline const char *
fault_injection_when_2_str(const struct fault_injection *flti)
{
    switch (flti->flti_when)
    {
    case FAULT_INJECT_PERIOD_one_time_only:
        return "one_time_only";
    case FAULT_INJECT_PERIOD_every_time:
        return "every_time";
    case FAULT_INJECT_PERIOD_per_freq_in_seconds:
        return "freq_in_seconds";
    case FAULT_INJECT_PERIOD_fixed_number_of_times:
        return "fixed_number_of_times";
    default:
        break;
    }

    return "unknown";
}

struct fault_injection *
fault_injection_lookup(const enum fault_inject_entries id);

static inline void
fault_injection_apply_info(struct fault_injection *flt, const char *file,
                           const char *func, unsigned int lineno)
{
    if (!flt || !file || !func || !lineno || flt->flti_file)
        return;                              // already installed..

    flt->flti_file = file;
    flt->flti_func = func;
    CONST_OVERRIDE(unsigned int, flt->flti_lineno, lineno);
}

static inline bool
fault_injection_evaluate(struct fault_injection *flti)
{
    if (!flti || !flti->flti_enabled)
        return false;

    bool fire = false;

    switch (flti->flti_when)
    {
    case FAULT_INJECT_PERIOD_one_time_only:
        fire = true;
        flti->flti_enabled = false;
        break;
    case FAULT_INJECT_PERIOD_every_time:
        fire = true;
        break;
    case FAULT_INJECT_PERIOD_per_freq_in_seconds:
        if ((niova_realtime_coarse_clock_get_sec() - flti->flti_last) >
            flti->flti_freq_seconds)
            fire = true;
        break;
    case FAULT_INJECT_PERIOD_fixed_number_of_times:
        fire = true;
        if (!--flti->flti_num_remaining)
            flti->flti_enabled = false;
        break;
    default:
        break;
    }

    if (fire)
        flti->flti_last = niova_realtime_coarse_clock_get_sec();

    return fire;
}

#if defined NIOVA_FAULT_INJECTION_ENABLED
#define FAULT_INJECT_CB(id, callback)                                   \
    ({                                                                  \
        static struct fault_injection_stub fis;                         \
        if (niova_atomic_cas(&fis.fis_atomic, 0, 1))                    \
        {                                                               \
            NIOVA_ASSERT(!fis.fis_flti);                                \
            fis.fis_flti = fault_injection_lookup(FAULT_INJECT_##id);   \
            NIOVA_ASSERT(fis.fis_flti);                                 \
            fault_injection_apply_info(fis.fis_flti, __FILE__, __func__, \
                                       __LINE__);                       \
        }                                                               \
        fis.fis_flti->flti_cond_exec_cnt++;                             \
        bool fire = fault_injection_evaluate(fis.fis_flti);             \
        if (fire)                                                       \
        {                                                               \
            fis.fis_flti->flti_inject_cnt++;                            \
            callback;                                                   \
        }                                                               \
        fire;                                                           \
    })
#else
#define	FAULT_INJECT_CB(id, callback) false
#endif

#define FAULT_INJECT(id) FAULT_INJECT_CB(id, )

init_ctx_t
fault_injection_init(void)  __attribute__
    ((constructor (FAULT_INJECT_CTOR_PRIORITY)));

#endif
