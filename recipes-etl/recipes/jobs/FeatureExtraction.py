from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numbers

output_path = "output/feature_extraction_result.parquet"

def extract_difficulty(*durations):
    if False in [isinstance(x, numbers.Number) for x in durations] or len(durations) == 0:
        return "Unknown"
    total_minutes = sum(durations)
    if total_minutes > 60:
        return "Hard"
    elif total_minutes <= 60 and total_minutes >= 30:
        return "Medium"
    elif total_minutes < 30:
        return "Easy"

    return "Unknown"

def add_difficulty_feature(df):
    udf_extract_difficulty = udf(extract_difficulty, returnType=StringType())
    difficultyAddedDf = df.withColumn("difficulty", udf_extract_difficulty("prepTime_minutes", "cookTime_minutes"))

    return difficultyAddedDf

def analyze(sc, sqlContext, args):
  filePath = args[0]
  preprocessedDf = sqlContext.read.load(filePath)
  difficultyAddedDf = add_difficulty_feature(preprocessedDf)

  difficultyAddedDf.write.save(output_path)
  return output_path
