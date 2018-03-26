from pyspark.sql.functions import udf
from pyspark.sql.types import *
import isodate

def parse_iso_duration(isodate_str):
    if isodate_str == None or isodate_str == "":
        return None
    return isodate.parse_duration(isodate_str).seconds/60

def parse_duration_columns(df, columns):
    udf_duration_parse = udf(parse_iso_duration, returnType=FloatType())
    parsedDf = df
    for column in columns:
        parsedDf = parsedDf.withColumn(column + "_minutes", udf_duration_parse(column))
    return parsedDf

def filter_with_keyword(df, column, keyword):
    return df.filter(df[column].like("%" + keyword + "%"))

def analyze(sc, sqlContext, args):
  filePath = args[0]
  rawDf = sqlContext.read.json(filePath)
  filteredDf = filter_with_keyword(rawDf, "ingredients", "Chilies")
  parsedDf = parse_duration_columns(filteredDf, ["prepTime", "cookTime"])

  parsedDf.write.save("preprocess_result.parquet")
  return parsedDf
