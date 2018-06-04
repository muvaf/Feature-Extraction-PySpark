from jobs import Preprocess, FeatureExtraction, OutputPublisher

def analyze(sc, sqlContext, args):
    '''Connects jobs with their input and output and executes them in a pipeline manner'''
    filePath = args[0]
    preprocessed_df = Preprocess.analyze(sc, sqlContext, [filePath])
    feature_extracted_df = FeatureExtraction.analyze(sc, sqlContext, [], input_df=preprocessed_df)
    output_path = OutputPublisher.analyze(sc, sqlContext, [], input_df=feature_extracted_df)

    return output_path
