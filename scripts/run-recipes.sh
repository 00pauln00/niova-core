#!/bin/bash

HOLON_LIBS=${1}
BIN_PATH=${2}
LOG_PATH=${3}
NPEERS=${4}
RECIPE_FILE=${5}
APP_TYPE=${6}
ENABLE_COALESCED_WR=${7}
GO_PATH=${8}
ENABLE_SYNC=${9}

export ANSIBLE_LOOKUP_PLUGINS=$HOLON_LIBS
export PYTHONPATH=$HOLON_LIBS
export NIOVA_BIN_PATH="$BIN_PATH/libexec/niova"
export CGO_LDFLAGS="-L$BIN_PATH/lib"
export CGO_CFLAGS="-I$BIN_PATH/include/niova"
export LD_LIBRARY_PATH="$BIN_PATH/lib"
export PATH="$PATH:$GO_PATH"

while IFS= read -r line; do
   recipe_list+=("$line")
done <$RECIPE_FILE
for recipe in "${recipe_list[@]}"
do
   if [ $# -eq 8 ]
   then
      #If the condition match run recipes in async.
      ansible-playbook -e 'srv_port=4000' -e npeers=$NPEERS -e dir_path=$LOG_PATH -e 'client_port=14000' -e recipe=$recipe -e 'backend_type=pumicedb' -e app_name=$APP_TYPE -e coalesced_wr=$ENABLE_COALESCED_WR holon.yml
   else
      #else run recipes in sync
      ansible-playbook -e 'srv_port=4000' -e npeers=$NPEERS -e dir_path=$LOG_PATH -e 'client_port=14000' -e recipe=$recipe -e 'backend_type=pumicedb' -e app_name=$APP_TYPE -e coalesced_wr=$ENABLE_COALESCED_WR -e sync=$ENABLE_SYNC holon.yml
   fi

   if [ $? -ne 0 ]
   then
      echo "Recipe: $recipe failed"
      exit 1
   fi
   echo "Recipe: $recipe completed successfully!"
   rm -rf $LOG_PATH/*
done

