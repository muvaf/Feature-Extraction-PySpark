from pyspark.sql import SQLContext

def analyze(sc, args):
  print("Preprocess is running")
  sqlContext = SQLContext(sc)
  filePath = "./data/recipes.json"
  rawDf = sqlContext.read.json(filePath)
  filteredDf = rawDf.filter(rawDf.ingredients.like("%Chilies%") | rawDf.ingredients.like("%Chiles%") | rawDf.ingredients.like("%Chili%"))

  filteredDf.write.save("filteredData.parquet")
