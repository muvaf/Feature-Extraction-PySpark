from pyspark.sql.functions import udf
from pyspark.sql.types import *
import isodate

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

def filter_with_regex(df, column, regex):
    return df.filter(df[column].rlike(regex))

def analyze(sc, sqlContext, args, input_df=None):
    if input_df == None:
        filePath = args[0]
        input_df = sqlContext.read.json(filePath)
    filtered_df = filter_with_regex(input_df, "ingredients", "[^a-zA-Z][cC][hH][iI][lL][iesIES]")
    parsed_df = parse_duration_columns(filtered_df, ["prepTime", "cookTime"])

    return parsed_df
