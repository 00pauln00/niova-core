/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */

#include <signal.h>
#include <backtrace.h>
#include <stdio.h>

#include "log.h"

static struct backtrace_state *niovaBacktraceState;
static pthread_mutex_t niovaBacktraceMutex = PTHREAD_MUTEX_INITIALIZER;
static int niovaBacktraceStackNum;

static void
niova_backtrace_error_cb(void *data, const char *msg, int errnum)
{
    fprintf(stderr, "data=%p msg=%s errno=%d", data, msg, errnum);
}

static int
niova_backtrace_simple_cb(void *data, uintptr_t pc)
{
    fprintf(stderr, "data=%p pc=%p\n", data, (void *)pc);

    return 0;
}

static int
niova_backtrace_full_cb(void *data, uintptr_t pc, const char *filename, int lineno,
                        const char *function)
{
    (void)data;

    if (pc != -1ULL && (filename || function))
        fprintf(stderr, "#%d\t0x%016lx in %s () at %s:%d\n",
                niovaBacktraceStackNum++, pc, function, filename, lineno);

    return 0;
}

void
niova_backtrace_dump_pc(uintptr_t pc)
{
    pthread_mutex_lock(&niovaBacktraceMutex);
    niovaBacktraceStackNum = 0;
    if (niovaBacktraceState)
        backtrace_pcinfo(niovaBacktraceState, pc, niova_backtrace_full_cb,
                         niova_backtrace_error_cb, NULL);

    pthread_mutex_unlock(&niovaBacktraceMutex);
}

void
niova_backtrace_dump_simple(void)
{
    pthread_mutex_lock(&niovaBacktraceMutex);
    niovaBacktraceStackNum = 0;

    if (niovaBacktraceState)
        backtrace_simple(niovaBacktraceState, 0, niova_backtrace_simple_cb,
                         niova_backtrace_error_cb, NULL);

    pthread_mutex_unlock(&niovaBacktraceMutex);
}

void
niova_backtrace_dump(void)
{
    pthread_mutex_lock(&niovaBacktraceMutex);
    niovaBacktraceStackNum = 0;

//    static char buf[4096] = {0};

    if (niovaBacktraceState)
        backtrace_full(niovaBacktraceState, 0, niova_backtrace_full_cb,
                       niova_backtrace_error_cb, NULL);

    pthread_mutex_unlock(&niovaBacktraceMutex);
}

static void
niova_backtrace_sighandler(int signum)
{
    niova_backtrace_dump();

    FATAL_IF((signum != SIGUSR2), "exiting on signal %d", signum);
}

static void
niova_backtrace_install_sighandler_for_sig(int signum)
{
    struct sigaction oldact;
    FATAL_IF_strerror((sigaction(signum, NULL, &oldact)), "sigaction: ");

    struct sigaction newact = oldact;
    newact.sa_handler = &niova_backtrace_sighandler;
    newact.sa_flags = 0;
    sigemptyset(&newact.sa_mask);

    FATAL_IF_strerror((sigaction(signum, &newact, NULL)), "sigaction: ");
}

static void
niova_backtrace_install_sighandler(void)
{
    static bool handlers_installed = false;
    if (handlers_installed)
        return;

    handlers_installed = true;

    niova_backtrace_install_sighandler_for_sig(SIGTERM);
    niova_backtrace_install_sighandler_for_sig(SIGSEGV);
    niova_backtrace_install_sighandler_for_sig(SIGUSR2);
    niova_backtrace_install_sighandler_for_sig(SIGBUS);
    niova_backtrace_install_sighandler_for_sig(SIGFPE);
}

void
niova_backtrace_print(void)
{
    if (niovaBacktraceState)
        backtrace_print(niovaBacktraceState, 0, stderr);
}

static init_ctx_t NIOVA_CONSTRUCTOR(BACKTRACE_SUBSYS_CTOR_PRIORITY)
niova_backtrace_init(void)
{
    niovaBacktraceState = backtrace_create_state(NULL, 0, NULL, NULL);
    NIOVA_ASSERT(niovaBacktraceState);

    niova_backtrace_install_sighandler();
}

static destroy_ctx_t NIOVA_DESTRUCTOR(BACKTRACE_SUBSYS_CTOR_PRIORITY)
niova_backtrace_destroy(void)
{
    return;
}
