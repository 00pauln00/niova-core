#!/usr/bin/env bash
NODE_NAME=Node$ID
echo $NODE_NAME

cd niovakv_server
./proxy -pa gossipNodes -u "e410fbf8-eba6-11eb-9378-5732a1244e6f" -e 4000 -l "logs/niovakv_server.log" -c ./config -n ${NODE_NAME}  > "logs/niovakv_server_output.log" 2>&1

ls PMDBConfig/
