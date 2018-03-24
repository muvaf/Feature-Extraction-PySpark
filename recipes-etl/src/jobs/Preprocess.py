from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import isodate

def parse_duration_columns(isodate_str):
    return isodate.parse_duration(isodate_str).seconds/60

def analyze(sc, args):
  print("Preprocess is running")
  sqlContext = SQLContext(sc)
  filePath = "./data/recipes.json"
  rawDf = sqlContext.read.json(filePath)
  udf_duration_parse = udf(parse_duration_columns, returnType=FloatType())
  filteredDf = rawDf.filter(rawDf.ingredients.like("%Chilies%") | rawDf.ingredients.like("%Chiles%") | rawDf.ingredients.like("%Chili%"))
  prepTimeParsedDf = filteredDf.withColumn("prepTimeMinutes", udf_duration_parse("prepTime")).drop("prepTime")
  cookTimeParsedDf = prepTimeParsedDf.withColumn("cookTimeMinutes", udf_duration_parse("cookTime")).drop("cookTime")

  cookTimeParsedDf.write.save("durationParsedData.parquet")
  return cookTimeParsedDf
