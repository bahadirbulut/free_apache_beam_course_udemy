import os
import apache_beam as beam

print("Data:")
print(os.system('cat ../Apache_Beam_Data/Peter_Piper.txt'))

words = ['peter', 'piper', 'pickled', 'picked', 'peck', 'pepper']


def findword(element):
    if element in words:
        return True


p1 = beam.Pipeline()

freq = (
        p1
        | beam.io.ReadFromText('../Apache_Beam_Data/Peter_Piper.txt')
        | beam.FlatMap(lambda record: record.split(' '))
        | beam.Filter(findword)
        | beam.Map(lambda record: (record, 1))
        | beam.CombinePerKey(sum)
        | beam.io.WriteToText('output/twister')
)
p1.run()

print("Result:")
print(os.system('cat output/twister*'))
