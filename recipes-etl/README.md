# Hellofresh Take Home Q2

Versions of packages:
* Apache Spark 1.6.0
* Python 3.5.2

## How to run?

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

Keep in mind that the root directory when running is `dist` folder, so when you specify a data source, you either need to specify an absolute path or a path that is relative to `dist` folder

Output will be on `dist/output/output_result.parquet`
