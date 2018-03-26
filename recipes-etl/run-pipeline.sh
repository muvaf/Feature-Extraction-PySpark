#!/bin/bash
MASTER_URL="local[*]"
DATA_SOURCE="../data/recipes.json"

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -u|--url)
    MASTER_URL="$2"
    shift # past argument
    shift # past value
    ;;
    -i|--input)
    DATA_SOURCE="$2"
    shift # past argument
    shift # past value
    ;;
esac
done

cd dist
spark-submit --master $MASTER_URL --py-files jobs.zip,libs.zip main.py --job ProductionPipeline --job-args $DATA_SOURCE
