# Requested output schema is likely to be defined by the client.
outputFields = ["name", "ingredients", "url", "image", "cookTime", "recipeYield", "datePublished", "prepTime", "description", "difficulty"]
output_path = "output/output.parquet"

def analyze(sc, sqlContext, args, input_df=None):
    '''Selects only given columns and writes as parquet file to the given output path'''
    if input_df == None:
        filePath = args[0]
        input_df = sqlContext.read.load(filePath)
    output_df = input_df.select(outputFields)

    output_df.write.save(output_path)
    return output_path
