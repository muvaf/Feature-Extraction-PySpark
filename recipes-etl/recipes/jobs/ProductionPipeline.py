from jobs import Preprocess, FeatureExtraction, OutputPublisher

def analyze(sc, sqlContext, args):
  filePath = args[0]
  preprocessed_df = Preprocess.analyze(sc, sqlContext, [filePath])
  feature_extracted_df = FeatureExtraction.analyze(sc, sqlContext, [], input_df=preprocessed_df)
  output_path = OutputPublisher.analyze(sc, sqlContext, [], input_df=feature_extracted_df)

  return output_path
