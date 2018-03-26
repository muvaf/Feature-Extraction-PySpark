from test.shared.PySparkTestCase import PySparkTestCase
from recipes.jobs.Preprocess import *
from pyspark.sql.types import *

class PreprocessTest(PySparkTestCase):

    def test_filter_with_regex(self):
        list_of_entries = [("name1", "a little bit enchiliada"),("name2", "some chili"),("name3", "more Chilies "),("name4", "salad with chiles"),("name5", "more of hilies"),("name6", "additional chilies")]
        schema_for_df = StructType([StructField("name", StringType(), True),StructField("ingredients", StringType(), True)])
        sample_rdd = self.sc.parallelize(list_of_entries)
        sample_df = self.sqlContext.createDataFrame(sample_rdd, schema_for_df)
        passed_entries = ["name2", "name3", "name4", "name6"]
        
        filtered_df = filter_with_regex(sample_df, "ingredients", "[^a-zA-Z][cC][hH][iI][lL][iesIES]")
        result_list = filtered_df.collect()
        self.assertEqual(len(result_list), 4)
        for row in result_list:
            self.assertTrue(row.name in passed_entries)

        empty_rdd = self.sc.parallelize([])
        empty_df = self.sqlContext.createDataFrame(empty_rdd, sample_df.schema)
        filtered_df = filter_with_regex(empty_df, "ingredients", "[^a-zA-Z][cC][hH][iI][lL][iesIES]")
        result_list = filtered_df.collect()
        self.assertEqual(len(result_list), 0)

    def test_parse_duration_columns(self):
        list_of_entries = [("name1", "PT5M", "PT10M"),("name2", "PT", "PT10M"),("name3", "PT1H", "PT"),("name4", "PT1H", "PT12H"),("name5", "PT", "PT"), ("name6", None, None)]
        schema_for_df = StructType([StructField("name", StringType(), True),StructField("prepTime", StringType(), True),StructField("cookTime", StringType(), True)])
        sample_rdd = self.sc.parallelize(list_of_entries)
        sample_df = self.sqlContext.createDataFrame(sample_rdd, schema_for_df)

        parsed_df = parse_duration_columns(sample_df, ["prepTime", "cookTime"])
        result_list = parsed_df.collect()
        for row in result_list:
            if row.name == "name1":
                self.assertEqual(row.prepTime_minutes, 5.0)
                self.assertEqual(row.cookTime_minutes, 10.0)
            elif row.name == "name2":
                self.assertEqual(row.prepTime_minutes, 0.0)
                self.assertEqual(row.cookTime_minutes, 10.0)
            elif row.name == "name3":
                self.assertEqual(row.prepTime_minutes, 60.0)
                self.assertEqual(row.cookTime_minutes, 0.0)
            elif row.name == "name4":
                self.assertEqual(row.prepTime_minutes, 60.0)
                self.assertEqual(row.cookTime_minutes, 720.0)
            elif row.name == "name5":
                self.assertEqual(row.prepTime_minutes, 0.0)
                self.assertEqual(row.cookTime_minutes, 0.0)
            elif row.name == "name6":
                self.assertEqual(row.prepTime_minutes, None)
                self.assertEqual(row.cookTime_minutes, None)

        empty_rdd = self.sc.parallelize([])
        empty_df = self.sqlContext.createDataFrame(empty_rdd, schema_for_df)
        parsed_df = parse_duration_columns(empty_df, ["prepTime", "cookTime"])
        result_list = parsed_df.collect()
        self.assertEqual(len(result_list), 0)



if __name__ == '__main__':
    unittest.main()
