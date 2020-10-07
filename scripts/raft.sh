#!/bin/sh

if [ $# -ne 2 ]
then
    echo "Usage:  $0 <Raft-UUID> <Peer-UUID>"
    exit 1
fi

export NIOVA_LOCAL_CTL_SVC_DIR=${NIOVA_LOCAL_CTL_SVC_DIR:-test/ctl-svr-test-inputs/}
export NIOVA_LOG_LEVEL=${NIOVA_LOG_LEVEL:-2}

RAFT_UUID=${1}
PEER_UUID=${2}

echo $RAFT_UUID \
    | egrep ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ \
            > /dev/null

if [ $? -ne 0 ]
then
    echo "Parameter '$RAFT_UUID' is not a valid UUID"
    exit 1
fi

echo $PEER_UUID \
    | egrep ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ \
            > /dev/null

if [ $? -ne 0 ]
then
    echo "Parameter '$PEER_UUID' is not a valid UUID"
    exit 1
fi

#gdb --args
#gdb -ex=r --args \
#./raft-server -a -r ${RAFT_UUID} -u ${PEER_UUID} 2> /tmp/${PEER_UUID}.peer

LD_LIBRARY_PATH=. gdb -ex=r --args \
./pumicedb-server-test -a -r ${RAFT_UUID} -u ${PEER_UUID} 2> /tmp/${PEER_UUID}.peer
