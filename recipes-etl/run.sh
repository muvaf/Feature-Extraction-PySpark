cd dist
spark-submit --py-files jobs.zip,libs.zip main.py --job WordCount
