from jobs import Preprocess, FeatureExtraction, OutputPublisher

def analyze(sc, sqlContext, args):
  filePath = args[0]
  preprocessed_path = Preprocess.analyze(sc, sqlContext, [filePath])
  feature_extracted_path = FeatureExtraction.analyze(sc, sqlContext, [preprocessed_path])
  output_path = OutputPublisher.analyze(sc, sqlContext, [feature_extracted_path])

  return output_path
