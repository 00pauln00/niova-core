/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE         /* See feature_test_macros(7) */

#include "common.h"
#include "log.h"
#include "random.h"

#include <getopt.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <time.h>
#include <pthread.h>
#include <linux/limits.h>

#define IC_BLOCKS_PER_MMAP_REGION 16
#if 1
#define IC_MMAP_SIZE (1048576)
#else
#define IC_MMAP_SIZE (65536)
#endif
#define IC_IO_SIZE   (IC_MMAP_SIZE / IC_BLOCKS_PER_MMAP_REGION)
#define IC_IO_SIZE_IN_WORDS (IC_IO_SIZE / sizeof(uint64_t))
#define IC_PAGE_SIZE_IN_WORDS (4096 / sizeof(uint64_t))
#define IC_NUM_IOs   (IC_MMAP_SIZE / IC_IO_SIZE)

#define OPTS "f:cshd:mt"

#define VERIFY_DEPTH IC_IO_SIZE_IN_WORDS
#define VERIFY_DEEP 0

#define IC_FIFO_READ(ic, buffer, len)                                   \
    read((ic)->ic_fifo_fd[(ic)->ic_server ? 0 : 1], buffer, len)

#define IC_FIFO_WRITE(ic, buffer, len)                                  \
    write((ic)->ic_fifo_fd[(ic)->ic_server ? 1 : 0], buffer, len)

static txn_id_t clientTxn;

static pthread_t bwReporter;
static pthread_t clientRecv;//, serverRecv;

struct ipcmmap_config
{
    char *ic_file_name;
    int   ic_fifo_fd[2];
    int   ic_mmap_fd;
    void *ic_mmap_region;
    bool  ic_server;
    bool  ic_vmsplice;
    bool  ic_recv_thread;
};

struct ipcmmap_op
{
    txn_id_t io_txn;
    size_t   io_len;
    uint32_t io_idx:31,
             io_pending:1;
} PACKED;

static struct ipcmmap_op ioOps[IC_NUM_IOs];

static void ipc_mmap_print_help(const int error)
{
    fprintf(error ? stderr : stdout,
            "client_mmap -f filename [-s] [-c] [-d log-level] [-m]\n");
    exit(error);
}

static struct ipcmmap_config ic;

static char *
ipc_mmap_get_region_for_op(struct ipcmmap_config *ic,
                           const struct ipcmmap_op *op)
{
    char *buffer = (char *)ic->ic_mmap_region;

    return &buffer[op->io_idx * IC_IO_SIZE];
}

static void
ipc_mmap_fifos(struct ipcmmap_config *ic)
{
    if (!ic->ic_server)
    {
        int i;
        for (i = 0; i < 2; i++)
        {
            char fifo_fname[PATH_MAX];
            snprintf(fifo_fname, PATH_MAX, "%s-%d-fifo", ic->ic_file_name, i);

            if (!ic->ic_server)
            {
                int rc = mkfifo(fifo_fname, 0700);
                if (rc == -1 && errno != EEXIST)
                    log_msg(LL_FATAL, "mkfifo() failed: %s", strerror(errno));
            }
        }
    }

    int i;
    for (i = 0; i < 2; i++)
    {
        char fifo_fname[PATH_MAX];
        snprintf(fifo_fname, PATH_MAX, "%s-%d-fifo", ic->ic_file_name, i);

        int flags;
        if (!i)
            flags = ic->ic_server ? O_RDONLY : O_WRONLY;
        else
            flags = ic->ic_server ? O_WRONLY : O_RDONLY;

        ic->ic_fifo_fd[i] = open(fifo_fname, flags);
        log_msg(LL_WARN, "opened %d", flags);


        if (ic->ic_fifo_fd[i] < 0)
            log_msg(LL_FATAL, "open() failed: %s", strerror(errno));
    }
}

static void
ipc_mmap_file(struct ipcmmap_config *ic)
{
    ic->ic_mmap_fd =
        open(ic->ic_file_name, (O_RDWR | O_CREAT | O_TRUNC), 0700);

    if (ic->ic_mmap_fd < 0)
        log_msg(LL_FATAL, "open(%s, ...) failed:  %s",
                ic->ic_file_name, strerror(errno));

    if (ftruncate(ic->ic_mmap_fd, (off_t)IC_MMAP_SIZE))
        log_msg(LL_FATAL, "frtuncate() failed:  %s", strerror(errno));

    ic->ic_mmap_region = mmap(NULL, (size_t)IC_MMAP_SIZE,
                              PROT_READ | PROT_WRITE, MAP_SHARED,
                              ic->ic_mmap_fd, 0);

    if ((ssize_t)ic->ic_mmap_region == -1)
        log_msg(LL_FATAL, "mmap() failed: %s", strerror(errno));
}

int
ipc_mmap_prep_file(struct ipcmmap_config *ic)
{
    if (!ic->ic_file_name)
        ipc_mmap_print_help(EINVAL);

    if (!ic->ic_vmsplice ||
        ((!ic->ic_server) && ic->ic_vmsplice))
        ipc_mmap_file(ic);

    ipc_mmap_fifos(ic);

    return 0;
}


static void
ipc_mmap_getopts(int argc, char **argv, struct ipcmmap_config *ic)
{
    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 's':
            ic->ic_server = true;
            break;
        case 'f':
            ic->ic_file_name = optarg;
            break;
        case 'h':
            ipc_mmap_print_help(0);
            break;
        case 'd':
            log_level_set(atoi(optarg));
            break;
        case 'm':
            ic->ic_vmsplice = true;
            break;
        case 't':
            ic->ic_recv_thread = true;
            break;
        default:
            log_msg(LL_ERROR, "invalid option: %c", opt);
            ipc_mmap_print_help(EINVAL);
            break;
        }
    }
}

static void
ipc_mmap_client_prepare_buffer(char *buffer, const txn_id_t txn,
                               const uint32_t idx)
{
    uint64_t *buffer_uint64 = (uint64_t *)buffer;
    uint64_t src_buffer[IC_IO_SIZE_IN_WORDS];
    size_t i;

#if VERIFY_DEEP
    for (i = 0; i < VERIFY_DEPTH; i += 1)
        src_buffer[i] = txn ^ idx ^ i;
#else
    for (i = 0; i < VERIFY_DEPTH; i += IC_PAGE_SIZE_IN_WORDS)
        src_buffer[i] = txn ^ idx ^ i;
#endif

    memcpy(buffer_uint64, src_buffer, IC_IO_SIZE);
}

static void
ipc_mmap_server_check_buffer(const char *buffer, const txn_id_t txn,
                             const uint32_t idx)
{
    uint64_t *buffer_uint64 = (uint64_t *)buffer;

    size_t i, inc_val;

#if VERIFY_DEEP
    inc_val = 1;
#else
    inc_val = IC_PAGE_SIZE_IN_WORDS;
#endif

    for (i = 0; i < VERIFY_DEPTH; i += inc_val)
    {
#if 0
        NIOVA_ASSERT(buffer_uint64[i] == (txn ^ idx ^ i));
#else
        if (buffer_uint64[i] != (txn ^ idx ^ i))
            log_msg(LL_ERROR, "got %lx expected %lx",
                    buffer_uint64[i], (txn ^ idx ^ i));

        //log_msg(LL_TRACE, "%lx %u %zu", txn, idx, i);
#endif
    }
}

static int
ipc_mmap_client_get_buffer_idx(const txn_id_t txn, const size_t len)
{
    static int last_offset = 0;

    int i;
    for (i = 0; i < IC_NUM_IOs; i++)
    {
        const int idx = (i + last_offset) % IC_NUM_IOs;

        if (!ioOps[idx].io_pending)
        {
            ioOps[idx].io_pending = true;
            ioOps[idx].io_txn = txn;
            ioOps[idx].io_len = len;
            ioOps[idx].io_idx = idx;

            last_offset = idx + 1;
            return idx;
        }
    }
    // Nothing was found.
    return -1;
}

static void
ipc_mmap_client_io_complete(struct ipcmmap_config *ic, const int ncompletions)
{
    struct ipcmmap_op io_op;

    int i;
    for (i = 0; i < ncompletions; i++)
    {
        int rc = IC_FIFO_READ(ic, &io_op, sizeof(io_op));

        enum log_level lvl = (rc != sizeof(io_op)) ? LL_FATAL : LL_DEBUG;

        log_msg(lvl, "read() rc=%d: %s",
                rc, rc != sizeof(io_op) ? strerror(errno) : "");

        NIOVA_ASSERT(ioOps[io_op.io_idx].io_pending)
        NIOVA_ASSERT(ioOps[io_op.io_idx].io_txn == io_op.io_txn);
        NIOVA_ASSERT(ioOps[io_op.io_idx].io_len == io_op.io_len);

        ioOps[io_op.io_idx].io_pending = false;

        log_msg(LL_DEBUG, "io_op=%lx idx=%u", io_op.io_txn, io_op.io_idx);
    }
}

static void
ipc_mmap_client_vmsplice_write(struct ipcmmap_config *ic, char *buf_addr,
                               const struct ipcmmap_op *op)
{
    struct iovec iov = {.iov_base = buf_addr, .iov_len = op->io_len};

    int rem_io = op->io_len;
    while (rem_io)
    {
        int rc = vmsplice(ic->ic_fifo_fd[0], &iov, 1, 0);
        if (rc < 0)
            log_msg(LL_FATAL, "rc=%d: %s", rc, strerror(errno));

        rem_io -= rc;
        iov.iov_base = buf_addr + (op->io_len - rem_io);
        iov.iov_len = rem_io;
    }
}

static void *
ipc_mmap_client_bw_reporter_thread(void *arg)
{
    (void)arg;

    txn_id_t past_txn = clientTxn;

    while (1)
    {
        sleep(1);

        txn_id_t current_txn = clientTxn;

        size_t bw = ((current_txn - past_txn) * IC_IO_SIZE) >> 20;

        past_txn  = current_txn;

        fprintf(stdout, "BW MiB/s:\t%zu\n", bw);
    }

    return NULL;
}

static void
ipc_mmap_client_bw_reporter(void)
{
    int rc = pthread_create(&bwReporter, NULL,
                            ipc_mmap_client_bw_reporter_thread, NULL);
    if (rc)
        log_msg(LL_FATAL, "rc=%d: %s", rc, strerror(errno))
}


static void *
ipc_mmap_client_recv_thread(void *arg)
{
    struct ipcmmap_config *ic = arg;
    struct ipcmmap_op io_op;

    while (1)
    {
        int rc = IC_FIFO_READ(ic, &io_op, sizeof(io_op));

        enum log_level lvl = (rc != sizeof(io_op)) ? LL_FATAL : LL_DEBUG;

        log_msg(lvl, "read() rc=%d: %s",
                rc, rc != sizeof(io_op) ? strerror(errno) : "");

        NIOVA_ASSERT(ioOps[io_op.io_idx].io_pending)
        NIOVA_ASSERT(ioOps[io_op.io_idx].io_txn == io_op.io_txn);
        NIOVA_ASSERT(ioOps[io_op.io_idx].io_len == io_op.io_len);

        ioOps[io_op.io_idx].io_pending = false;

        log_msg(LL_DEBUG, "io_op=%lx idx=%u", io_op.io_txn, io_op.io_idx);
    }
    return NULL;
}

static void
ipc_mmap_client_recv(struct ipcmmap_config *ic)
{
    if (ic->ic_recv_thread)
    {
        int rc = pthread_create(&clientRecv, NULL,
                                ipc_mmap_client_recv_thread, (void *)ic);
        if (rc)
            log_msg(LL_FATAL, "rc=%d: %s", rc, strerror(errno));
    }
}

static void
ipc_mmap_client(struct ipcmmap_config *ic)
{
    ipc_mmap_client_bw_reporter();
    ipc_mmap_client_recv(ic);

    for (; ;)
    {
        int idx = ipc_mmap_client_get_buffer_idx(clientTxn, IC_IO_SIZE);
        if (idx < 0)
        {
            if (!ic->ic_recv_thread)
                ipc_mmap_client_io_complete(ic, 1);
            else
                usleep(1);

            continue;
        }

        ioOps[idx].io_pending = true;
        ioOps[idx].io_idx = idx;
        ioOps[idx].io_txn = clientTxn++;
        ioOps[idx].io_len = IC_IO_SIZE;

        char *buf_addr = ipc_mmap_get_region_for_op(ic, &ioOps[idx]);

        ipc_mmap_client_prepare_buffer(buf_addr, ioOps[idx].io_txn, idx);

        int rc = IC_FIFO_WRITE(ic, &ioOps[idx], sizeof(struct ipcmmap_op));
        if (rc != sizeof(struct ipcmmap_op))
            log_msg(LL_FATAL, "rc=%d", rc);

        log_msg(LL_DEBUG, "write() rc=%d: %s",
                rc, rc < 0 ? strerror(errno) : "");

        if (ic->ic_vmsplice)
            ipc_mmap_client_vmsplice_write(ic, buf_addr, &ioOps[idx]);
    }
}

static void
ipc_mmap_memcpy_server(struct ipcmmap_config *ic, const struct ipcmmap_op *op,
                       char *sink_buffer)
{
    const char *buf_addr = ipc_mmap_get_region_for_op(ic, op);

    memcpy(sink_buffer, buf_addr, IC_IO_SIZE);

    ipc_mmap_server_check_buffer(sink_buffer, op->io_txn, op->io_idx);
}

static void
ipc_vmsplice_read_memcpy_server(struct ipcmmap_config *ic,
                                const struct ipcmmap_op *op, char *sink_buffer)
{
    int rem_io = op->io_len;
    while (rem_io)
    {
        int rc = IC_FIFO_READ(ic, sink_buffer + (op->io_len - rem_io), rem_io);
        if (rc < 0)
            log_msg(LL_FATAL, "read rc=%d", rc);

        rem_io -= rc;
    }
}

static void
ipc_mmap_server(struct ipcmmap_config *ic)
{
    char sink_buffer[IC_IO_SIZE];
    struct ipcmmap_op io_op;

    for (; ;)
    {
        int rc = IC_FIFO_READ(ic, &io_op, sizeof(struct ipcmmap_op));
        if (!rc)
            continue;

        if (rc != sizeof(struct ipcmmap_op))
            log_msg(LL_FATAL, "read rc=%d", rc);

        if (ic->ic_vmsplice)
            ipc_vmsplice_read_memcpy_server(ic, &io_op, sink_buffer);
        else
            ipc_mmap_memcpy_server(ic, &io_op, sink_buffer);

        // do something here.
        rc = IC_FIFO_WRITE(ic, &io_op, sizeof(struct ipcmmap_op));

        if (rc != sizeof(struct ipcmmap_op))
            log_msg(LL_FATAL, "write rc=%d", rc);

        log_msg(LL_DEBUG, "write() rc=%d: %s",
                rc, rc < 0 ? strerror(errno) : "");
    }
}

int
main(int argc, char **argv)
{
    log_level_set(LL_WARN);

    ipc_mmap_getopts(argc, argv, &ic);
    ipc_mmap_prep_file(&ic);

    if (ic.ic_server)
        ipc_mmap_server(&ic);
    else
        ipc_mmap_client(&ic);
}
