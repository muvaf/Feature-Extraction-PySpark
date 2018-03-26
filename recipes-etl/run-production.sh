cd dist
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job Preprocess --job-args ../test/data/recipes.json
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job FeatureExtraction --job-args preprocess_result.parquet
