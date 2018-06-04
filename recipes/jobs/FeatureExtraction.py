from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numbers

def extract_difficulty(*durations):
    '''Returns difficulty relative to sum of the durations as string'''
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
    '''Adds difficulty column that is extracted from the sum of prepTime and cookTime'''
    udf_extract_difficulty = udf(extract_difficulty, returnType=StringType())
    difficultyAddedDf = df.withColumn("difficulty", udf_extract_difficulty("prepTime_minutes", "cookTime_minutes"))

    return difficultyAddedDf

def analyze(sc, sqlContext, args, input_df=None):
    '''Adds features to the input as seperate columns'''
    if input_df == None:
        filePath = args[0]
        input_df = sqlContext.read.load(filePath)
    difficultyAddedDf = add_difficulty_feature(input_df)

    return difficultyAddedDf
