#!/bin/bash

CTL_DIR=/tmp/.niova/
ITERATIONS=1
VERBOSE=""
WATCH=0
EDIT=0

function print_help()
{
    echo \
        "Usage:  $0 [-d ctl-dir (${CTL_DIR})] [-i iterations (${ITERATIONS})] [-e (launch edit terminal)] [-w (launch watch terminal)] <cmd-file>"
    exit
}

while getopts ":vd:i:weh" opt; do
  case ${opt} in
    d )
        CTL_DIR=$OPTARG
        if [ ! -d $CTL_DIR ]
        then
            echo "ctl-interface path '$CTL_DIR' does not exist or is not a directory."
            exit
        fi
        ;;
    e )
        EDIT=1
        ;;
    h )
        print_help
        ;;
    i )
        ITERATIONS=$OPTARG
        RES=`echo $ITERATIONS | egrep ^[1-9]$\|[1-9][0-9]+$`
        if [ $? -ne 0 ]
        then
            echo "Iteration value '$ITERATIONS' does not appear to be numeric."
            exit
        fi
        ;;
    w )
        WATCH=1
        ;;
    v )
        VERBOSE="v"
        ;;
    \? )
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

CMD_FILE=$1
if [ ! -f $CMD_FILE ]
then
    echo "cmd-file '$CMD_FILE' does not exist or is not a regular file."
    exit
fi

RES=`egrep ^OUTFILE $CMD_FILE`
if [ $? -ne 0 ]
then
    echo "cmd-file '$CMD_FILE' does contain the OUTFILE directive"
    exit
fi

RES=`egrep ^OUTFILE[[:space:]][/][/a-zA-Z0-9_] $CMD_FILE`
if [ $? -ne 0 ]
then
    echo "cmd-file '$CMD_FILE' has malformed OUTFILE directive"
    exit
fi

OUT_FILE=`egrep ^OUTFILE $CMD_FILE | awk '{print $2}' | sed s/\\\///g`

if [ $WATCH -eq 1 ]
then
    REGEX_PATH=${CTL_DIR}[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/output/$OUT_FILE
    CMD="watch -n 1 find ${CTL_DIR} -regextype posix-egrep -regex \"${REGEX_PATH}\" -exec cat {} \\;"
    gnome-terminal --zoom .75 -- $CMD > /dev/null
fi

if [ $EDIT -eq 1 ]
then
    gnome-terminal -- vim $CMD_FILE > /dev/null
fi

while [ $ITERATIONS -gt 0 ]
do
    for i in `find ${CTL_DIR} | \
        egrep [a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`;
    do
        cp -f${VERBOSE} $CMD_FILE $i/input;
    done

    let ITERATIONS=$ITERATIONS-1
    if [ $ITERATIONS -gt 0 ]
    then
        sleep 1
    fi
done
