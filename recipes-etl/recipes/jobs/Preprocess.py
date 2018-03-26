from pyspark.sql.functions import udf
from pyspark.sql.types import *
import isodate

output_path = "output/preprocess_result.parquet"
def parse_iso_duration(isodate_str):
    if isodate_str == None or isodate_str == "":
        return None
    return isodate.parse_duration(isodate_str).seconds/60

def parse_duration_columns(df, columns):
    udf_duration_parse = udf(parse_iso_duration, returnType=FloatType())
    parsed_df = df
    for column in columns:
        parsed_df = parsed_df.withColumn(column + "_minutes", udf_duration_parse(column))
    return parsed_df

def filter_with_keyword(df, column, keyword):
    return df.filter(df[column].like("%" + keyword + "%"))

def analyze(sc, sqlContext, args):
  filePath = args[0]
  raw_df = sqlContext.read.json(filePath)
  filtered_df = filter_with_keyword(raw_df, "ingredients", "Chilies")
  parsed_df = parse_duration_columns(filtered_df, ["prepTime", "cookTime"])

  parsed_df.write.save(output_path)
  return output_path
