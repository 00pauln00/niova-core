/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef __FILE_UTIL_H
#define __FILE_UTIL_H 1

ssize_t
file_util_open_and_read(int dirfd, const char *file_name, char *output_buf,
                        size_t output_size, int *ret_fd);

#endif
