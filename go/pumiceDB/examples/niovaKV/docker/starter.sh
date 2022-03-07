#!/usr/bin/env bash
raft=($(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft))
IFS="." read -r -a rft <<< ${raft[0]}
RAFT_UUID=${rft[0]}

line=$(( $ID + 1 ))
server=$(sed -n "$line p" $NIOVA_LOCAL_CTL_SVC_DIR/${raft[0]})
IFS=" " read -r -a serv <<< $server
SERVER_UUID=${serv[1]}
echo $SERVER_UUID


clients=($(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft_client))
IFS="." read -r -a cli <<< ${clients[ID]}
CLIENT_UUID=${cli[0]}

NODE_NAME=Node$ID
echo $NODE_NAME

cd pmdb_server
./pmdbServer -r ${RAFT_UUID} -u ${SERVER_UUID} -l "logs/pmdb_server.log" > "logs/pmdb_output.log" 2>&1 &
cd ../niovakv_server
./proxy -e 4000 -r ${RAFT_UUID} -u ${CLIENT_UUID} -l "logs/niovakv_server.log" -c ./config -n ${NODE_NAME}  > "logs/niovakv_server_output.log" 2>&1
