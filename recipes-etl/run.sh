cd dist
spark-submit --py-files jobs.zip,libs.zip main.py --job Preprocess
spark-submit --py-files jobs.zip,libs.zip main.py --job FeatureExtraction
