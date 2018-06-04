import sys, argparse
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

parser = argparse.ArgumentParser()
parser.add_argument('--expected', type=str, required=True)
parser.add_argument('--actual', type=str, required=True)
args = parser.parse_args()

spark_conf = SparkConf().setAppName("IntegrationTest")
sc = SparkContext(conf=spark_conf)
sqlContext = SQLContext(sc)

df_1 = sqlContext.read.parquet(args.expected)
df_2 = sqlContext.read.parquet(args.actual)

difference_1 = df_1.subtract(df_2)
difference_2 = df_2.subtract(df_1)

count1 = difference_1.count()
count2 = difference_2.count()
sc.stop()

if count1 == 0 and count2 == 0:
    print("*****\nINTEGRATION TEST PASSED.\n*****")
    sys.exit(0)
else:
    print("*****\nINTEGRATION TEST FAILED.\n*****")
    sys.exit(1)
