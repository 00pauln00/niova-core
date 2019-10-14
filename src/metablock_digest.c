/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */

#include "common.h"
#include "metablock.h"

#include <string.h>
#include <openssl/sha.h>

static int
metablock_digest_calc(const struct mb_hash *mb_hash, const char *data,
                      size_t len, unsigned char *computed_digest)
{
    if (!mb_hash || !data || !computed_digest || !len)
        return -EINVAL;

    /* For now, only a single type is supported.
     */
    if (mb_hash->mh_type != MB_DIGEST_SHA1)
        return -ENOTSUP;

    SHA_CTX ctx;
    if (!SHA1_Init(&ctx) ||
        !SHA1_Update(&ctx, (const void *)data, len) ||
        !SHA1_Final(computed_digest, &ctx))
        return -EBADE;

    return 0;
}

/**
 * metablock_digest_check - calculate the digest of 'data' and compare it to
 *    the digest value stored in mb_hash.
 * @mb_hash:  the stored digest value which should match the calculated value
 *    derived from 'data'.
 * @data:  the data buffer on which the digest is calculated.
 * @len:  the data buffer's length.
 */
int
metablock_digest_check(const struct mb_hash *mb_hash, const char *data,
                       size_t len)
{
    unsigned char computed_digest[SHA_DIGEST_LENGTH];

    int rc = metablock_digest_calc(mb_hash, data, len, computed_digest);
    if (rc)
        return rc;

    return memcmp((const void *)mb_hash->mh_bytes,
                  (const void *)computed_digest, SHA_DIGEST_LENGTH) ?
        -EBADMSG : 0;
}

/**
 * metablock_digest_generate - generate and store the digest value into the
 *    provided mb_hash structure.
 * @mb_hash:  the mb_hash structure to which the result will be stored.
 * @data:  the input data from which the digest is derived.
 * @len:  length of the input data.
 * @digest_type:  the type of hash to use for generating the digest.
 */
int
metablock_digest_generate(struct mb_hash *mb_hash, const char *data,
                          size_t len, mb_digest_type_t digest_type)
{
    if (!mb_hash || !data || !len || digest_type)
        return -EINVAL;

    memset(mb_hash, 0, sizeof(struct mb_hash));

    mb_hash->mh_type = digest_type;

    return metablock_digest_calc(mb_hash, data, len, mb_hash->mh_bytes);
}
