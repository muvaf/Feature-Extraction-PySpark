from pyspark.sql.functions import udf
from pyspark.sql.types import *
import isodate

def parse_iso_duration(isodate_str):
    '''Parses ISO8601 durations into minutes.'''
    if isodate_str == None or isodate_str == "":
        return None
    return isodate.parse_duration(isodate_str).seconds/60

def parse_duration_columns(df, columns):
    '''Parses given ISO8601 duration columns and adds new values as seperate columns by appending '_minutes' to the column name '''
    udf_duration_parse = udf(parse_iso_duration, returnType=FloatType())
    parsed_df = df
    for column in columns:
        parsed_df = parsed_df.withColumn(column + "_minutes", udf_duration_parse(column))
    return parsed_df

def filter_with_regex(df, column, regex):
    '''Filters given columns with given regex expression'''
    return df.filter(df[column].rlike(regex))

def analyze(sc, sqlContext, args, input_df=None):
    '''Parses needed columns from string to relative type for further processing'''
    if input_df == None:
        filePath = args[0]
        input_df = sqlContext.read.json(filePath)
    filtered_df = filter_with_regex(input_df, "ingredients", "[^a-zA-Z][cC][hH][iI][lL][iesIES]")
    parsed_df = parse_duration_columns(filtered_df, ["prepTime", "cookTime"])

    return parsed_df
