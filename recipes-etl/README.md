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

Output will be on `dist/output/output_result.parquet`


python3.5
spark 1.6.0
