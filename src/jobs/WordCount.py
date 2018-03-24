def to_pairs(word):
    return word, 1

def analyze(sc, args):
  print("Running wordcount")
  textPath = "./data/text.txt"

  words = sc.textFile(textPath)
  pairs = words.map(to_pairs)
  ordered = pairs.sortBy(lambda pair: pair[1], ascending=False)
  print(ordered.collect())
