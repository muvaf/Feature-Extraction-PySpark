cd dist
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job ProductionPipeline --job-args ../data/recipes.json
