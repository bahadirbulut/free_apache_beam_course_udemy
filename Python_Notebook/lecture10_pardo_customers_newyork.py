import os
import apache_beam as beam

print("Data:")
print(os.system('cat ../Apache_Beam_Data/Customers_age.txt'))

p1 = beam.Pipeline()


class SplitRow(beam.DoFn):
    def process(self, element):
        return [element.split(',')]


class FilterCustomer(beam.DoFn):
    def process(self, element):
        if element[2] == 'NY' and int(element[3]) > 20:
            return [element]


customers = (
        p1
        | beam.io.ReadFromText('../Apache_Beam_Data/Customers_age.txt')
        | beam.ParDo(SplitRow())
        | beam.ParDo(FilterCustomer())
        | beam.io.WriteToText('output/customers_pardo')
)
p1.run()

print("Result:")
print(os.system('cat output/customers_pardo*'))
