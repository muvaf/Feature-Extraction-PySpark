import sys, argparse, unittest
from test.shared.PySparkTestCase import PySparkTestCase
from recipes.jobs.FeatureExtraction import *


class IntegrationTestCheck(PySparkTestCase):

    def test_is_same(self):
        df_1 = self.sqlContext.read.parquet("test/data/expected_output.parquet")
        df_2 = self.sqlContext.read.parquet("dist/output/output.parquet")

        difference_1 = df_1.subtract(df_2)
        self.assertEqual(difference_1.count(), 0)

        difference_2 = df_2.subtract(df_1)
        self.assertEqual(difference_2.count(), 0)

if __name__ == '__main__':
    unittest.main()
