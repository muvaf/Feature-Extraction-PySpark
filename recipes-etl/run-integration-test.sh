#!/bin/bash
./run-pipeline.sh
python test/integration-test-check.py --expected test/data/expected_output.parquet --actual dist/output/output.parquet
