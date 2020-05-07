#!/bin/sh

NUM_SERVERS=5
NUM_CLIENTS=3

SERVER_STARTING_PORT=6000
CLIENT_PORT_ADD=20

function print_help()
{
    echo "Usage:  $0 [-s num-servers] [-c num-clients] [-p starting-port] <dir>"
    exit
}

if [ $# -lt 1 ]
then
    print_help
    exit 1
fi

RES=`which uuid`
if [ $? -ne 0 ]
then
    echo "Cannot locate uuid command"
    exit
fi

while getopts ":s:c:p:h" opt; do
case ${opt} in
    h )
        print_help
        ;;
    s )
        NUM_SERVERS=$OPTARG
        RES=`echo $NUM_SERVERS | egrep ^[1-9]$\|[1-9][0-9]+$`
        if [ $? -ne 0 ]
        then
            echo "num-servers '$NUM_SERVERS' is invalid."
            exit
        fi

        if [ $NUM_SERVERS -lt 3 ]
        then
            echo "Warning:  num-servers '$NUM_SERVERS' is too low for a working config (min-num-servers == 3)"
        fi
        ;;
    c )
        NUM_CLIENT=$OPTARG
        RES=`echo $NUM_CLIENTS | egrep ^[0-9]+$`
        if [ $? -ne 0 ]
        then
            echo "num-clients '$NUM_CLIENTS' is invalid."
            exit
        fi
        ;;
    p )
        SERVER_STARTING_PORT=$OPTARG
        RES=`echo $SERVER_STARTING_PORT | egrep ^[1-9]$\|[1-9][0-9]+$`
        if [ $? -ne 0 ]
        then
            echo "starting-port '$SERVER_STARTING_PORT' does not appear to be numeric."
            exit
        fi
        ;;
    ? )
        echo "Invalid option: '$OPTARG'" 1>&2
        echo ""
        print_help
        ;;
    : )
        echo "Invalid option: '$OPTARG' requires an argument" 1>&2
        ;;
  esac
done

# Adjust $# to account for 'getopts'
shift $((OPTIND -1))


if [ $# -lt 1 ]
then
    print_help
fi

DIR=$1
if [ ! -e $DIR ]
then
    mkdir -p $DIR
    if [ $? -ne 0 ]
    then
        exit
    fi
fi

# Re-check
if [ ! -d $DIR ]
then
    echo "target path '$DIR' does not exist or is not a directory."
    exit
fi

RAFT_UUID=`uuid`
RAFT_DIR=${DIR}/${RAFT_UUID}.raft_config

mkdir -p $RAFT_DIR
if [ $? -ne 0 ]
then
    exit
fi

mkdir -p /var/tmp/${RAFT_UUID}
if [ $? -ne 0 ]
then
    exit
fi

# Generate the UUIDs for the server peers
x=0
while [ $x -lt ${NUM_SERVERS} ]
do
    SERVER_UUID[${x}]=`uuid`
    let x=$x+1
done

# Build the raft file
echo "RAFT ${RAFT_UUID}" > ${RAFT_DIR}/${RAFT_UUID}.raft
x=0
while [ $x -lt ${NUM_SERVERS} ]
do
    echo "PEER ${SERVER_UUID[${x}]}" >> ${RAFT_DIR}/${RAFT_UUID}.raft
    let x=$x+1
done

# Build server peer files
x=0
while [ $x -lt ${NUM_SERVERS} ]
do
    echo \
"RAFT        $RAFT_UUID
IPADDR      127.0.0.1
PORT        ${SERVER_STARTING_PORT}
CLIENT_PORT $((${SERVER_STARTING_PORT}+${CLIENT_PORT_ADD}))
STORE       /var/tmp/${RAFT_UUID}/${SERVER_UUID[${x}]}.raftdb" \
    >> $RAFT_DIR/${SERVER_UUID[${x}]}.peer

    let SERVER_STARTING_PORT=${SERVER_STARTING_PORT}+1
    let x=$x+1
done

# Build client files
x=0
while [ $x -lt ${NUM_CLIENTS} ]
do
    echo \
"RAFT        $RAFT_UUID
IPADDR      127.0.0.1
CLIENT_PORT $((${SERVER_STARTING_PORT}+${CLIENT_PORT_ADD}))" \
    >> $RAFT_DIR/`uuid`.raft_client

    let SERVER_STARTING_PORT=${SERVER_STARTING_PORT}+1
    let x=$x+1
done

#echo "Raft Config:  $RAFT_DIR/"
echo "$RAFT_UUID"

export RAFT_MK_CONFIG_UUID=$RAFT_UUID
