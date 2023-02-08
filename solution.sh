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

while getopts "elqad:" opt; do
    case $opt in
    e)
        extract=1
        ;;
    l)
        load=1
        ;;
    q)
        query=1
        ;;
    a)
        extract=1
        load=1
        query=1
        ;;
    d)
        date="$OPTARG"
        ;;
    \?)
        echo "Invalid option: -$OPTARG$" >&2
        exit 1
        ;;
    :)
        echo "Option -$OPTARG requires an argument." >&2
        exit 1
        ;;
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
