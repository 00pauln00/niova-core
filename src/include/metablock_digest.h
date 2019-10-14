/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */

struct mb_hash;

enum mb_digest_type
{
    MB_DIGEST_SHA1 = 0,
};

typedef enum mb_digest_type mb_digest_type_t;

int
metablock_digest_check(const struct mb_hash *mb_hash, const char *data,
                       size_t len);

int
metablock_digest_generate(struct mb_hash *mb_hash, const char *data,
                          size_t len, mb_digest_type_t digest_type);
