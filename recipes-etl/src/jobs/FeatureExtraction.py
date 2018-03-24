from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *

def extract_difficulty(*durations):
    total_minutes = sum(durations)
    if total_minutes > 60:
        return "Hard"
    elif total_minutes <= 60 and total_minutes >= 30:
        return "Medium"
    elif total_minutes < 30:
        return "Easy"

    return "Unknown"

def analyze(sc, args):
  print("Feature Extraction is running")
  sqlContext = SQLContext(sc)
  filePath = "durationParsedData.parquet"
  preprocessedDf = sqlContext.read.load(filePath)

  udf_extract_difficulty = udf(extract_difficulty, returnType=StringType())
  difficultyAddedDf = preprocessedDf.withColumn("difficulty", udf_extract_difficulty("prepTimeMinutes", "cookTimeMinutes"))

  difficultyAddedDf.write.save("featurizedData.parquet")
  
  return difficultyAddedDf
