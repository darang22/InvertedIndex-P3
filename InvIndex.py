from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os

WORD_RE = re.compile(r"[\w']+")
class MRWordFrequencyCount(MRJob):

  def getKey(item):
    return item[0]

  def mapper(self, _, line):
    for word in WORD_RE.findall(line):
      fileName = os.environ['mapreduce_map_input_file']
      yield (word,fileName), 1

  def reducer(self, fileName, rep):
    yield fileName[0], (fileName[1], sum(rep))

  def reducer2(self, word, values):
    l = list(values)
    #Returns the top 10 documents where the key word is repeated the most
    l = sorted(l, key=lambda x: x[1], reverse = True)[:10]
    yield word, l

  def steps(self):
    return [
      MRStep(mapper = self.mapper, reducer = self.reducer),
      MRStep(reducer = self.reducer2)]
if __name__ == '__main__':
    MRWordFrequencyCount.run()































""""

WORD_RE = re.compile(r"[\w']+")
class MRWordFrequencyCount(MRJob):
  def mapper(self, _, line):
    for word in WORD_RE.findall(line):
      fileName = os.environ['mapreduce_map_input_file']
      yield word,fileName

  def reducer(self, word, fileName):
      l = list(set(fileName))
      yield word,l


WORD_RE = re.compile(r"[\w']+")
class MRWordFrequencyCount(MRJob):
  def mapper(self, _, line):
    for word in WORD_RE.findall(line):
      fileName = os.environ['mapreduce_map_input_file']
      yield fileName,1

  def reducer(self, fileName, counts):
      yield fileName,sum(counts)

if __name__ == '__main__':
    MRWordFrequencyCount.run()


from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	se, ide, salario, year  = line.decode('utf-8','ignore').split()
        yield se, int(salario)

    def reducer(self, se, salario):
      l = list(salario)
      yield se, sum(l)/len(l)



if __name__ == '__main__':
    MRWordFrequencyCount.run()


from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
  se, ide, salario, year  = line.decode('utf-8','ignore').split()
        yield ide, int(salario)

    def reducer(self, ide, salario):
      l = list(salario)
      yield ide, sum(l)/len(l)



if __name__ == '__main__':
    MRWordFrequencyCount.run()



from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
  se, ide, salario, year  = line.decode('utf-8','ignore').split()
        yield se, ide

    def reducer(self, se, ide):
      l = list(ide)
      yield se, len(l)



if __name__ == '__main__':
    MRWordFrequencyCount.run()

"""
