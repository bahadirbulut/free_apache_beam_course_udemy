import os
import apache_beam as beam


class MyTransform(beam.PTransform):
    def expand(self, input_col):
        a = (
                input_col
                | 'Calculate Sum' >> beam.Map(CalculateSum)
                | 'Apply Formatting' >> beam.Map(FormatText)
        )
        return a


def SplitRow(input_element):
    return input_element.split(',')


def FilterBasedonCountry(countryName, input_element):
    return input_element[1] == countryName


def CalculateSum(elem):
    return elem[0], (int(elem[2]) + int(elem[3]) + int(elem[4]))


def FormatText(elem):
    return elem[0] + ' has received ' + str(elem[1]) + ' marks'


p1 = beam.Pipeline()

input_collection = (
        p1
        | beam.io.ReadFromText('../Apache_Beam_Data/students_marks.txt')
        | beam.Map(SplitRow)
)

US_pipeline = (
        input_collection
        | beam.Filter(lambda record: FilterBasedonCountry('US', record))
        | "Composite Transformation for US" >> MyTransform()
        | 'Writing results to US File' >> beam.io.WriteToText('output/US_Result')
)

India_pipeline = (
        input_collection
        | beam.Filter(lambda record: FilterBasedonCountry('IN', record))
        | "Composite Transformation for IN" >> MyTransform()
        | 'Writing results to India File' >> beam.io.WriteToText('output/IN_Result')
)

p1.run()

print('')
print("US Result: ")
print(os.system('cat output/US*'))

print('')
print("IN Result: ")
print(os.system('cat output/IN*'))

# ! cat US_Result-00000-of-00001
#
# ! cat IN_Result-00000-of-00001
