RAFT=$(basename $NIOVA_LOCAL_CTL_SVC_DIR | cut -d . -f 1)
CLIENT=$(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft_client | head -1 | cut -d. -f1)

GDB="gdb -ex=r --args"

echo NIOVA_LOG_LEVEL=5 ./raft-client -r $RAFT -u $CLIENT
NIOVA_LOG_LEVEL=5 $GDB ./raft-client -r $RAFT -u $CLIENT
