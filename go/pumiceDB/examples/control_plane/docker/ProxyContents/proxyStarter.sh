#!/usr/bin/env bash
NODE_NAME=Node$ID
echo $NODE_NAME



clients=($(ls $NIOVA_LOCAL_CTL_SVC_DIR | grep raft_client))
IFS="." read -r -a cli <<< ${clients[ID]}
CLIENT_UUID=${cli[0]}
echo $CLIENT_UUID

cd niovakv_server
./proxy -pa gossipNodes -u ${CLIENT_UUID} -e 4000 -l "logs/niovakv_server.log" -c ./config -n ${NODE_NAME}  > "logs/niovakv_server_output.log" 2>&1
