from test.shared.PySparkTestCase import PySparkTestCase
from recipes.jobs.FeatureExtraction import *

class PreprocessTest(PySparkTestCase):

    def test_extract_difficulty(self):
        difficulty = extract_difficulty(10.0, 10.0)
        self.assertEqual(difficulty, "Easy")
        difficulty = extract_difficulty(10.0, 25.0)
        self.assertEqual(difficulty, "Medium")
        difficulty = extract_difficulty(30.0, 40.0)
        self.assertEqual(difficulty, "Hard")
        difficulty = extract_difficulty(10.0, 25.0, 50.0)
        self.assertEqual(difficulty, "Hard")
        difficulty = extract_difficulty(10.0)
        self.assertEqual(difficulty, "Easy")
        difficulty = extract_difficulty(None, None)
        self.assertEqual(difficulty, "Unknown")
        difficulty = extract_difficulty(None)
        self.assertEqual(difficulty, "Unknown")
        difficulty = extract_difficulty()
        self.assertEqual(difficulty, "Unknown")

    def test_add_difficulty_feature(self):
        list_of_entries = [("name1", 10.0, 0.0),("name2", 5.0, 50.0),("name3", None, 5.0),("name4", 50.0, 50.0),("name5", None, None), ("name6", 0.0, 0.0)]
        schema_for_df = StructType([StructField("name", StringType(), True),StructField("prepTime_minutes", FloatType(), True),StructField("cookTime_minutes", FloatType(), True)])
        sample_rdd = self.sc.parallelize(list_of_entries)
        sample_df = self.sqlContext.createDataFrame(sample_rdd, schema_for_df)

        difficulty_added_df = add_difficulty_feature(sample_df)
        result_list = difficulty_added_df.collect()
        for row in result_list:
            if row.name == "name1":
                self.assertEqual(row.difficulty, "Easy")
            elif row.name == "name2":
                self.assertEqual(row.difficulty, "Medium")
            elif row.name == "name3":
                self.assertEqual(row.difficulty, "Unknown")
            elif row.name == "name4":
                self.assertEqual(row.difficulty, "Hard")
            elif row.name == "name5":
                self.assertEqual(row.difficulty, "Unknown")
            elif row.name == "name6":
                self.assertEqual(row.difficulty, "Easy")

        empty_rdd = self.sc.parallelize([])
        empty_df = self.sqlContext.createDataFrame(empty_rdd, schema_for_df)
        difficulty_added_df = add_difficulty_feature(empty_df)
        result_list = difficulty_added_df.collect()
        self.assertEqual(len(result_list), 0)

if __name__ == '__main__':
    unittest.main()
