AC_PREREQ(2.63)

m4_define([niova_core_major], [0])
m4_define([niova_core_minor], [01])
m4_define([niova_core_suffix], [0])
m4_define([niova_core_version],
          [niova_core_major.niova_core_minor.niova_core_suffix])

AC_INIT([niova_core],[niova_core_version],[],[],[pauln@niova.io])
LT_INIT
AC_CONFIG_AUX_DIR([.])
AM_PROG_AS
AM_INIT_AUTOMAKE([foreign subdir-objects -Wall -Wno-portability])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS([niova_core_config.h])
AC_CONFIG_FILES([Makefile])

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CC_C99

AC_C_CONST
AC_C_INLINE
AC_TYPE_INT8_T
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT8_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

AC_HEADER_STDC
AC_CHECK_HEADERS([pthread.h], [], [AC_MSG_ERROR([failed to locate pthread.h])], [])
AC_CHECK_HEADERS([unistd.h], [], [AC_MSG_ERROR([failed to locate unistd.h])], [])
AC_CHECK_HEADERS([string.h], [], [AC_MSG_ERROR([failed to locate string.h])], [])
AC_CHECK_HEADERS([stdlib.h], [], [AC_MSG_ERROR([failed to locate stdlib.h])], [])
AC_CHECK_HEADERS([uuid/uuid.h], [], [AC_MSG_ERROR([failed to locate uuid/uuid.h])], [])

# libuuid
AC_CHECK_LIB([uuid],[uuid_compare],,[AC_MSG_ERROR([Could not find uuid_compare])])

#pthread
AC_CHECK_LIB([pthread],[pthread_join],,[AC_MSG_ERROR([Could not find pthread_join])])

# rocksdb
AC_CHECK_LIB([rocksdb],[rocksdb_readoptions_create],,[AC_MSG_ERROR([Could not find rocksdb])])

AC_OUTPUT
