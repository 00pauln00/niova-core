#!/usr/bin/env bash
raft=($(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft))
IFS="." read -r -a rft <<< ${raft[0]}
RAFT_UUID=${rft[0]}

line=$(( $ID + 1 ))
server=$(sed -n "$line p" $NIOVA_LOCAL_CTL_SVC_DIR/${raft[0]})
IFS=" " read -r -a serv <<< $server
SERVER_UUID=${serv[1]}
echo $SERVER_UUID

cd pmdb_server
./pmdbServer -g gossipNodes -r ${RAFT_UUID} -u ${SERVER_UUID} -l "logs/pmdb_server.log" > "logs/pmdb_output.log" 2>&1
