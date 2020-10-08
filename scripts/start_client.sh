. /tmp/.raft_dir
echo NIOVA_LOCAL_CTL_SVC_DIR=$NIOVA_LOCAL_CTL_SVC_DIR

RAFT=$(basename $NIOVA_LOCAL_CTL_SVC_DIR | cut -d . -f 1)
CLIENT=$(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft_client | head -1 | cut -d. -f1)

GDB="gdb -ex=r --args"

CMD=./pumicedb-client-test

echo NIOVA_LOG_LEVEL=5 ./raft-client -r $RAFT -u $CLIENT
echo cp scripts/ctl-interface-cmds/pmdb-write.cmd /tmp/.niova/$CLIENT/input/
NIOVA_LOG_LEVEL=5 NIOVA_LOCAL_CTL_SVC_DIR=$NIOVA_LOCAL_CTL_SVC_DIR $GDB $CMD -r $RAFT -u $CLIENT

