# Hellofresh Take Home Q2

Versions of packages:
* Apache Spark 1.6.0
* Python 3.5.2 (3.6 will not work since it is supported by Spark 2.1.0 and upper versions)

## How to run?

You need to run the following to set the environment variables for unit tests to find pyspark module, if not set:
```
export SPARK_HOME=/usr/local/spark
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH
```

To run unit tests:
```
make libs
python -m unittest discover
```

To run production pipeline as requested by the question:
```
make production
./run-pipeline.sh
```
Command above will run by default on local using all the cores available.
You can use `--url` argument to pass a YARN master node url and `--input` argument to pass another data source.
Try to use double-quotes with the arguments since some characters might get misread such as brackets in 'local[4]'

Keep in mind that the root directory when running is `dist` folder, so when you specify a data source, you either need to specify an absolute path or a path that is relative to `dist` folder

Example with different data source and url:
```
./run-pipeline.sh --url "local[2]" --input "/Users/foo/Desktop/recipes.json"
```

Output will be on `dist/output/output_result.parquet`
