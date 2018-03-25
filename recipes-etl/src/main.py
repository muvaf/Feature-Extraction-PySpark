import os, sys, argparse, importlib
from pyspark import SparkContext, SparkConf

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

parser = argparse.ArgumentParser()
parser.add_argument('--job', type=str, required=True)
parser.add_argument('--job-args', nargs='*')
args = parser.parse_args()

# TODO: Make it runnable on YARN
spark_conf = SparkConf().setAppName(args.job)
sc = SparkContext(conf=spark_conf)
job_module = importlib.import_module('jobs.%s' % args.job)
job_module.analyze(sc, args.job_args)
