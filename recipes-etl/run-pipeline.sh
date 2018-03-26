#!/bin/bash
MASTER_URL = "local[*]"
DATA_SOURCE = "../data/recipes.json"
if [ "$1" != "" ]; then
    echo "Positional parameter 1 contains something"
else
    echo "Positional parameter 1 is empty"
fi
cd dist
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job ProductionPipeline --job-args ../data/recipes.json
