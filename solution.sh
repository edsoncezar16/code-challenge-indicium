#!/bin/bash

export CREDENTIALS_PATH='docker-compose.yml'
export CSV_PATH='data/order_details.csv'
export OUTPUT_DB_NAME='order_details'
export RESULTS_PATH='data/results.csv'

function check_exec {
    status=$?
    if [ "$status" != "0" ]; then
        echo "$1 failed."
        exit 1
    fi
}

function usage {
    echo "Usage: solution.sh [-a] [-e] [-l] [-q] [-d DATE]

    Options:
    -a, --all              All pipeline's operations are executed.
    -e, --extract          Extract data from from the provided sources.
    -l, --load             Load data to the output Postgres database.
    -q, --query            Query output database to show the orders and their details and stores the result to local disk.
    -d DATE, --date DATE   Define a date in the format 'YYYY-MM-DD' to execute the operations. Default: current date."
}

PARSED_ARGUMENTS=$(getopt -a -n solution -o aelqd: --long all,extract,load,query,date: -- "$@")

VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ]; then
  usage
fi

eval set -- "$PARSED_ARGUMENTS"
while :
do
  case "$1" in
    -a | --all)     extract=1
                    load=1
                    query=1; shift;;
    -e | --extract) extract=1; shift;;
    -l | --load)    load=1; shift;;
    -q | --query)   query=1; shift;;
    -d | --date)    date="$2"; shift 2;;
    --)             shift; break;; # end of arguments
    *) echo "Unexpected option: $1."
       usage ;;
  esac
done

if [ "$extract" = "1" ]; then
    python scripts/extract.py "$date"
    check_exec Extraction
fi

if [ "$load" = "1" ]; then
    python scripts/transform.py "$date"
    check_exec Transformation
    python scripts/load.py "$date"
    check_exec Loading
fi

if [ "$query" = "1" ]; then
    python scripts/query.py "$date"
    check_exec Querying
fi
