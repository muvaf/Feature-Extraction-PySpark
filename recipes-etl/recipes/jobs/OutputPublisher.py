
# Requested output schema is likely to be defined by the client.
outputFields = ["name", "ingredients", "url", "image", "cookTime", "recipeYield", "datePublished", "prepTime", "description", "difficulty"]
output_path = "output/output_result.parquet"

def analyze(sc, sqlContext, args):
  filePath = args[0]
  preprocessedDf = sqlContext.read.load(filePath)
  output_df = preprocessedDf.select(outputFields)

  output_df.write.save(output_path)
  return output_path
