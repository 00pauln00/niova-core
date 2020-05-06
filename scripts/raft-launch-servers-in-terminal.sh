#!/bin/sh

if [ $# -ne 1 ]
then
    echo "Usage:  $0 <Raft-UUID>"
    exit 1
fi

RAFT_SUFFIX=".raft"
RAFT_UUID=${1}

echo $RAFT_UUID \
    | egrep ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ \
            > /dev/null

if [ $? -ne 0 ]
then
    echo "Parameter '$RAFT_UUID' is not a valid UUID"
    exit 1
fi

CFG_FILE=`find . -type f -name ${RAFT_UUID}${RAFT_SUFFIX}`

if [ $? -ne 0 ] || [ "${CFG_FILE}x" == "x" ]
then
    echo "UUID '$RAFT_UUID' could not be found"
    exit 1
fi

# Set the ctl-svc dir based on the location of the raft conf file
export NIOVA_LOCAL_CTL_SVC_DIR=`dirname ${CFG_FILE}`

for i in $(cat ${CFG_FILE} | grep PEER | awk '{print $2}')
do
    echo "Found peer-uuid $i"
    gnome-terminal -- `dirname $0`/raft.sh ${RAFT_UUID} ${i}
done
