#!/bin/bash
make production
./run-pipeline.sh
python test/integration-test-check.py
