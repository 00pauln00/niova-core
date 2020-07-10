RAFT=$(basename $NIOVA_LOCAL_CTL_SVC_DIR | cut -d . -f 1)
CLIENT=$(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft_client | head -1 | cut -d. -f1)

echo NIOVA_LOG_LEVEL=2 ./raft-client -r $RAFT -u $CLIENT
NIOVA_LOG_LEVEL=2 ./raft-client -r $RAFT -u $CLIENT
