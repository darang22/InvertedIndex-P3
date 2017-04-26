from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol
import re
import os

import unicodedata
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


# Optional apostrophe only between characters (w)+ ('? w)+
#WORD_RE = re.compile(r"[^\W_]+[\S]?[\S]?[^\W_]+", re.UNICODE)
WORD_RE = re.compile(r"[^\W_]+(?:[\S]?[^\W_]+)*", re.UNICODE)

class MRWordFrequencyCount(MRJob):

  INPUT_PROTOCOL = TextValueProtocol
  def getKey(item):
    return item[0]

  def mapper(self, _, line):
    for word in WORD_RE.findall(line):
      fileName = os.environ['mapreduce_map_input_file']
      yield (word.lower(),fileName), 1

  def reducer(self, fileName, rep):
    yield fileName[0], (fileName[1], sum(rep))

  def reducer2(self, word, values):
    l = list(values)
    #Returns the top 10 documents where the key word is repeated the most
    l = sorted(l, key=lambda x: x[1], reverse = True)[:10]
    #Remove special characters
    output = unicodedata.normalize('NFD', word).encode('ascii','ignore')  
    yield output, str(l)

  def steps(self):
    return [
      MRStep(mapper = self.mapper, reducer = self.reducer),
      MRStep(reducer = self.reducer2)]
if __name__ == '__main__':
    MRWordFrequencyCount.run()

