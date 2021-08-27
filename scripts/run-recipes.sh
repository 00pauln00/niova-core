#!/bin/bash

HOLON_LIBS=${1}
BIN_PATH=${2}
LOG_PATH=${3}
NPEERS=${4}
starting_port=4000
ending_port=15000

export ANSIBLE_LOOKUP_PLUGINS=$HOLON_LIBS
export PYTHONPATH=$HOLON_LIBS
export NIOVA_BIN_PATH=$BIN_PATH

declare -a recipe_list=("leader_overthrow.yml"
                        "leader_self_depose.yml"
                        "pmdb_client_request_timeout_modification_and_retry.yml"
                        "pmdb_foreign_client_error_demonstration.yml"
                        "promoting_the_most_qualified_peer_to_lead_multi_peer_recovery.yml"
                        "rollback_during_startup.yml"
                        "selecting_the_correct_leader_at_boot_time.yml"
                        "completing_an_uncommitted_write_following_a_reboot.yml"
                        "election_timeout_modification.yml"
                        "pmdb_client_error_demonstration_standalone_client.yml"
                       )

for recipe in "${recipe_list[@]}"
do
   echo "find an open port to use"
   for i in $(seq $starting_port $ending_port); do
        if ! lsof -Pi :$i; then
            port_to_use=$i
            ansible-playbook -e srv_port=$port_to_use -e npeers=$NPEERS -e dir_path=$LOG_PATH -e client_port=$port_to_use -e recipe=$recipe -e 'backend_type=pumicedb' holon.yml
        elif [ "$i" == "$ending_port" ]; then
            echo "no port to use!"
        fi
   done

   if [ $? -ne 0 ]
   then
      echo "Recipe: $recipe failed"
      exit 1
   fi
   echo "Recipe: $recipe completed successfully!"
   rm -rf $LOG_PATH/*
done
