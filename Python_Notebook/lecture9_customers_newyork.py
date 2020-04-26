import os
import apache_beam as beam

print("Data:")
print(os.system('cat ../Apache_Beam_Data/Customers_age.txt'))

p1 = beam.Pipeline()
customers = (
    p1
    | beam.io.ReadFromText('../Apache_Beam_Data/Customers_age.txt')
    | beam.Map(lambda record: record.split(','))
    | beam.Filter(lambda record: record[2] == 'NY' and int(record[3]) > 20)
    | beam.io.WriteToText('output/customers')
)
p1.run()

print("Result:")
print(os.system('cat output/customers*'))
