/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */


#include "common.h"
#include "alloc.h"
#include "util.h"
#include "superblock.h"
#include "niosd_io.h"
#include "niosd_uuid.h"

REGISTRY_ENTRY_FILE_GENERATE;

static int
superblock_allocate(struct niosd_device *ndev)
{
    if (ndev->ndev_sb)
        return -EALREADY;

    ndev->ndev_sb = niova_calloc((size_t)1, sizeof(struct sb_header_data));

    return !ndev->ndev_sb ? -errno : 0;
}

int
superblock_format_new(struct niosd_device *ndev)
{
    if (ndev->ndev_status != NIOSD_DEV_STATUS_SB_INIT)
        return -EINVAL;

    int rc = superblock_allocate(ndev);
    if (rc)
        return rc;

    niosd_id_t niosd_id;
    niosd_uuid_generate(&niosd_id);

//    niosd_uuid_generate(&ndev->ndev_sb->sbh_niosd_id);

// create I/O calls to write the SB

    DBG_SB(LL_WARN, ndev->ndev_sb, "");

    return 0;
}

static void
superblock_read_done(struct niosd_io_request *niorq)
{
    niosd_io_request_destroy(niorq);
}

static void
superblock_read_error(const struct niosd_io_request *niorq)
{
    if (niorq_has_error(niorq))
    {
        struct niosd_device *ndev = niosd_ctx_to_device(niorq->niorq_ctx);
        struct sb_header_data *sb = ndev->ndev_sb;

        ndev->ndev_status = NIOSD_DEV_STATUS_SB_ERROR;

        DBG_NIOSD_REQ(LL_ERROR, niorq, "sb read error:  sb# %hhu",
                      sb_2_current_replica_num(sb));

        if (sb_2_current_replica_num(sb) > 0)
            DBG_SB(LL_ERROR, sb, "err %ld or %ld",
                   niorq->niorq_res, niorq->niorq_res2);
    }
}

/**
 * superblock_digest_check - compares the digest contents stored in mb_hash
 *    with the calculated digest from 'data'.
 *
 *    NOTE:  caller should ensure that the length value is valid for the given
 *           superblock type or version since metablock_digest_check() is not
 *           equipped for such a determination.
 */
//static int
int
superblock_digest_check(const struct mb_hash *mb_hash, const char *data,
                        size_t len)
{
    return metablock_digest_check(mb_hash, data, len);
}

//static int
int
superblock_verify_contents(struct niosd_device *ndev, const char *pblk_data)
{
    const struct sb_header_persistent *sb_hp =
        (const struct sb_header_persistent *)pblk_data;

    sb_replica_t current_sb_replica = sb_2_current_replica_num(ndev->ndev_sb);

    pblk_id_t current_sb_pblk = current_sb_replica ?
        sb_2_pblk_id(ndev->ndev_sb, current_sb_replica) : SB_PRIMARY_PBLK_ID;

    if (current_sb_pblk == PBLK_ID_ANY)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "invalid current_sb_pblk=%x",
                       current_sb_pblk);
        return -EINVAL;
    }

    /* Ensure the magic is correct for a superblock.
     */
    if (sb_hp->sbhp_header.sbh_magic != SB_MAGIC)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "sb-pblk=%x repl=%hhu invalid magic=%lx",
                       current_sb_pblk, current_sb_replica,
                       sb_hp->sbhp_header.sbh_magic);

        return -EBADF;
    }

//XXXX incomplete!
    return 0;
}

static int
superblock_initialize_from_ndev_pblk_data(struct niosd_device *ndev,
                                          const char *pblk_data)
{
    const struct sb_header_persistent *sb_hp =
        (const struct sb_header_persistent *)pblk_data;

    NIOVA_ASSERT(sb_hp);
//    NIOVA_ASSERT(sb_hp->sbhp_header.magic..);

    return 0;
}

/**
 * superblock_read_continue_cb - niosd I/O callback issued after reading a
 *   superblock pblk_id.  This call will
 *
 */
static void
superblock_read_continue_cb(struct niosd_io_request *niorq)
{
    NIOVA_ASSERT(niorq);

    struct niosd_device *ndev = niosd_ctx_to_device(niorq->niorq_ctx);
    struct sb_header_data *sb = ndev->ndev_sb;

    NIOVA_ASSERT(sb_2_current_replica_num(sb) < sb_2_replica_num(sb));

    if (niorq_has_error(niorq))
    {
        superblock_read_error(niorq);
        superblock_read_done(niorq);
        return;
    }

    sb_replica_t sb_replica = sb_2_current_replica_num(sb);

    if (!sb_replica)
        superblock_initialize_from_ndev_pblk_data(ndev,
                                                  niorq->niorq_sink_buf);

    DBG_NIOSD_REQ(LL_DEBUG, niorq, "current-replica=%hhu", sb_replica);

    sb_set_current_replica_num(sb, sb_replica + 1);
}

/**
 * superblock_read_submit - issues a request into the niosd_device to read
 *   the superblocks.  This call is non-blocking and returns success when the
 *   I/O request to read the first super block has been successfully issued.
 *   To determine whether logical superblock read operation completed
 *   successfully, the niosd_device status may be tested for
 *   NIOSD_DEV_STATUS_SB_ERROR.
 */
int
superblock_read_launch(struct niosd_device *ndev)
{
    if (ndev->ndev_status != NIOSD_DEV_STATUS_STARTUP_SB_SCAN)
        return -EINVAL;

    int rc = superblock_allocate(ndev);
    if (rc)
        return rc;

    char *sb_blk_buf = niova_malloc(sizeof(struct sb_header_persistent));
    if (!sb_blk_buf)
        return -errno;

    struct niosd_io_request *niorq =
        niova_malloc(sizeof(struct niosd_io_request));

    if (!niorq)
    {
        niova_free(sb_blk_buf);
        return -ENOMEM;
    }

    rc = niosd_io_request_init(niorq,
                               niosd_device_to_ctx(ndev,
                                                   NIOSD_IO_CTX_TYPE_SYSTEM),
                               SB_PRIMARY_PBLK_ID, NIOSD_REQ_TYPE_PREAD, 8,
                               sb_blk_buf, superblock_read_continue_cb, NULL);
    if (rc)
        return rc;

    rc = niosd_io_submit(&niorq, 1);

    return rc == 1 ? 0 : rc;
}
