#!/bin/bash

HOLON_LIBS=${1}
BIN_PATH=${2}
LOG_PATH=${3}
NPEERS=${4}
starting_Sport=4000
ending_Sport=4010
starting_Cport=14000
ending_Cport=14010

declare -a sport=()
declare -a cport=()

list1=$(seq $starting_Sport $ending_Sport)
list2=$(seq $starting_Cport $ending_Cport)

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

for i in ${list1[@]}
do
	if ! lsof -Pi :$i; then
        	sport+=($i)
        fi
        for j in ${list2[@]}
        do
        	if ! lsof -Pi :$j; then
                	cport+=($j)
                fi
        done
done

echo "Server Port: ${sport[0]}"
echo "Client Port: ${cport[0]}"

for recipe in "${recipe_list[@]}"
do
   ansible-playbook -e srv_port=${sport[0]} -e npeers=$NPEERS -e dir_path=$LOG_PATH -e client_port= -e recipe=${cport[0]} -e 'backend_type=pumicedb' holon.yml
   if [ $? -ne 0 ]
   then
      echo "Recipe: $recipe failed"
      exit 1
   fi
   echo "Recipe: $recipe completed successfully!"
   rm -rf $LOG_PATH/*
done

