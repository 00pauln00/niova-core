#!/bin/sh

export UUID=${1}

NIOVA_INOTIFY_PATH=/tmp/.niova/${UUID} \
NIOVA_LOCAL_CTL_SVC_DIR=test/ctl-svr-test-inputs/ \
NIOVA_LOG_LEVEL=2 \
gdb -ex=r --args \
./raft-server -r ae3f4a60-2038-11ea-ae5f-90324b2d1e85 \
-u ${UUID}  2> /tmp/${UUID}.peer
