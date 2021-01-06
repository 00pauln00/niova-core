#!/bin/sh

HOLON_LIBS=${1}
BIN_PATH=${2}
LOG_PATH=${3}
NPEERS=${4}

echo "Holon lib $HOLON_LIBS"
echo "All executatbles at: $BIN_PATH"
echo "holon log path: $LOG_PATH"
echo "Npeers: $NPEERS"

export ANSIBLE_LOOKUP_PLUGINS=$HOLON_LIBS
export PYTHONPATH=$HOLON_LIBS
export NIOVA_BIN_PATH=$BIN_PATH

ansible-playbook -e 'srv_port=4000' -e npeers=$NPEERS -e dir_path=$LOG_PATH -e 'client_port=14000' -e 'recipe=leader_overthrow.yml' -e 'backend_type=pumicedb' holon.yml
