# Feature Extraction Example with Apache Spark

This project serves as an example of how a production pipeline model could be built using best practices. It includes only feature extraction part, but other parts could easily be added.

Versions of packages:
* Apache Spark 1.6.0
* Python 3.5.2 (3.6 will not work since it is supported by Spark 2.1.0 and upper versions)

## How to run?

You need to run the following to set the environment variables for unit tests to find pyspark module, if not set:
```
export SPARK_HOME=/usr/local/spark
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH
```

## Production Pipeline

To run production pipeline as requested by the question:
```
make production
./run-pipeline.sh
```
Command above will run by default on local using all the cores available.
You can use `--master` argument to pass a master and `--input` argument to pass another data source.
Try to use double-quotes with the arguments since some characters might get misread such as brackets in 'local[4]'

Keep in mind that the root directory when running is `dist` folder, so when you specify a data source, you either need to specify an absolute path or a path that is relative to `dist` folder

Example with different data source and url:
```
./run-pipeline.sh --master "local[2]" --input "/Users/foo/Desktop/recipes.json"
```

Output is written to `dist/output/output_result.parquet`

### On YARN
Dependencies and job modules are packaged together in zip files. So, all nodes will have the same source code and modules in a cluster. To run on a YARN custer, specify `--master` as `yarn`:
```
./run-pipeline.sh --master yarn
```

## Unit Tests

To run unit tests:
```
make libs
python -m unittest discover
```
## Integration Test

Integration test checks if the output produced (resides in `dist/output/output_result.parquet`) is the same with the expected output located in `test/data/expected_output.parquet`. To run the integration test for the requested case:
```
python test/integration_test_check.py --expected test/data/expected_output.parquet --actual dist/output/output.parquet
```
