cd dist
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job Preprocess --job-args ../data/recipes.json
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job FeatureExtraction --job-args ./output/preprocess_result.parquet
spark-submit --master local[*] --py-files jobs.zip,libs.zip main.py --job OutputPublisher --job-args ./output/feature_extraction_result.parquet
