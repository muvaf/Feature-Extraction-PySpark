from test.shared.PySparkTestCase import PySparkTestCase
from recipes.jobs.Preprocess import *

class PreprocessTest(PySparkTestCase):

    def test_filter_with_keyword(self):
        sample_df = self.sqlContext.read.json("test/data/sample-data.json")
        filtered_df = filter_with_keyword(sample_df, "ingredients", "Chilies")
        result_list = filtered_df.collect()
        self.assertEqual(len(result_list), 3)
        for row in result_list:
            self.assertTrue(row.ingredients.find("Chilies") != -1)



if __name__ == '__main__':
    unittest.main()
