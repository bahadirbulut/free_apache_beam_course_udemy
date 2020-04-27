import apache_beam as beam
import os

words=['peter','piper','pickled','picked','peck','pepper']


# def SplitRow(element):
#   return element.split(' ')


def FindWord(element):
 if element in words:
    return True


class SplitRow(beam.DoFn):
  def process(self,element):
    return element.split(' ')


class CalculateFrequency(beam.DoFn):
  def process(self,element):
    (key,value) = element
    # print(element)
    return [(key,sum(value))]


p1 = beam.Pipeline()

freq = (
    p1
    | 'Read your input file' >> beam.io.ReadFromText('../Apache_Beam_Data/Peter_Piper.txt')
    | 'Split Records with Space' >> beam.ParDo(SplitRow())
    | 'Filtering records' >> beam.Filter(FindWord)
    | 'Create tupled records'>> beam.Map(lambda record: (record,1))
    | 'Group By Key'>> beam.GroupByKey()
    | 'Calculate Frequency of words' >> beam.ParDo(CalculateFrequency())
    | 'Write to a file' >> beam.io.WriteToText('output/twist_pardo')
)
p1.run()

print('Result:')
print(os.system('cat output/twist_pardo*'))