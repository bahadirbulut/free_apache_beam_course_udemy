import apache_beam as beam

p = beam.Pipeline()

even = {2, 4, 6, 8}
odd = {1, 3, 5, 7, 9}
name = ('John', 'Jim', 'Mary')

even_pc = p | 'Create Pcollection from Even number' >> beam.Create(even)
odd_pc = p | 'Create Pcollection from Odd Number' >> beam.Create(odd)
name_pc = p | 'Create Pcollection from Name' >> beam.Create(name)

result = ({even_pc, odd_pc, name_pc} | beam.Flatten()) #| beam.Map(print)
print_file = result | beam.Map(print)
write_to_file = result | beam.io.WriteToText('output/flatten', file_name_suffix='.txt')

p.run()
