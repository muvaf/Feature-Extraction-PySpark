import unittest
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        conf = SparkConf()\
            .setMaster("local[*]") \
            .setAppName(self.__name__)
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)

    @classmethod
    def tearDownClass(self):
        self.sc.stop()
