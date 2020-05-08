#!/bin/sh

WATCH=0
DRY_RUN=0
LAUNCH_START_PEER=0
LAUNCH_END_PEER=999

function print_help()
{
    echo "Usage:  $0 [-d (dry-run)] [-w (watch)] [-s starting-peer] [-e ending-peer] <Raft-UUID>"
    exit
}

if [ $# -lt 1 ]
then
    print_help
    exit 1
fi

while getopts ":s:e:hdw" opt; do
case ${opt} in
    h )
        print_help
        ;;
    d )
        DRY_RUN=1
        ;;
    s )
        LAUNCH_START_PEER=$OPTARG
        RES=`echo $LAUNCH_START_PEER | egrep ^[0-9]+$`
        if [ $? -ne 0 ]
        then
            echo "starting-peer '$LAUNCH_START_PEER' does not appear to be numeric."
            exit
        fi
        ;;
    e )
        LAUNCH_END_PEER=$OPTARG
        RES=`echo $LAUNCH_END_PEER | egrep ^[0-9]+$`
        if [ $? -ne 0 ]
        then
            echo "ending-peer '$LAUNCH_END_PEER' does not appear to be numeric."
            exit
        fi
        ;;
    w )
        WATCH=1
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

if [ $# -ne 1 ]
then
    print_help
fi

RAFT_SUFFIX=".raft"
RAFT_UUID=${1}

# Verify the raft arg is a real UUID
echo $RAFT_UUID \
    | egrep ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ \
            > /dev/null

if [ $? -ne 0 ]
then
    echo "Parameter '$RAFT_UUID' is not a valid UUID"
    exit 1
fi

SEARCH_PATHS[0]=.
SEARCH_PATHS[1]=/tmp
SEARCH_PATHS[2]=/etc
CFG_FILE=""

for i in ${SEARCH_PATHS[@]}
do
    TMP_CFG_FILE=`find ${i} -type f -name ${RAFT_UUID}${RAFT_SUFFIX} 2>/dev/null | head -1`
    if [ "${TMP_CFG_FILE}x" != "x" ]
    then
        echo ${TMP_CFG_FILE} \
            | egrep [a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}.raft$ \
                    > /dev/null
        if [ $? -ne 0 ]
        then
            continue
        else
            CFG_FILE=${TMP_CFG_FILE}
            break
        fi
    fi
done

if [ $? -ne 0 ] || [ "${CFG_FILE}x" == "x" ]
then
    echo "UUID '$RAFT_UUID' could not be found"
    exit 1
fi

RAFT_SCRIPT=""
for i in ${SEARCH_PATHS[@]}
do
    TMP_CFG_FILE=`find ${i} -type f -name raft.sh 2>/dev/null | head -1`
    if [ "${TMP_CFG_FILE}" != "raft.sh" ]
    then
        RAFT_SCRIPT=${TMP_CFG_FILE}
        break
    fi
done

if [ "${RAFT_SCRIPT}x" == "x" ]
then
    echo "raft.sh could not be found"
    exit 1
fi

# Set the ctl-svc dir based on the location of the raft conf file
export NIOVA_LOCAL_CTL_SVC_DIR=`dirname ${CFG_FILE}`

IDX=0

for i in $(cat ${CFG_FILE} | grep PEER | awk '{print $2}')
do
    if [ $LAUNCH_START_PEER -gt $IDX ] || [ $LAUNCH_END_PEER -lt $IDX ]
    then
        echo "Skip peer $i (position=${IDX})"
    else
        echo "Launch peer $i"
        if [ $DRY_RUN -ne 0 ]
        then
            echo "gnome-terminal -- ${RAFT_SCRIPT} ${RAFT_UUID} ${i}"
        else
            gnome-terminal -- ${RAFT_SCRIPT} ${RAFT_UUID} ${i} > /dev/null
        fi
    fi

    let IDX=$IDX+1
done


if [ $WATCH -ne 0 ]
then
    echo "GET /raft_root_entry/.*/.*/.*
GET /system_info/current_time
OUTFILE /raft-$$-${RANDOM}.out" >> /tmp/raft-$$.cmd
    `dirname $0`/ctl-interface-cmd-to-all.sh -e -w -i 100000 /tmp/raft-$$.cmd
fi
