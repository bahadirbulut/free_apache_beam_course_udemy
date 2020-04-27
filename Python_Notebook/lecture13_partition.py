import apache_beam as beam

p = beam.Pipeline()
number = {1, 2, 3, 4, 5, 6, 7, 8}


def partition_fn(element):
    return 0 if element % 2 == 0 else 1


number_pc = p | beam.Create(number) | beam.Partition(partition_fn, 2)

# number_pc[1] | 'Printing first partition' >> beam.Map(lambda x: print('First Part: ' + str(x))) # beam.Map(print)
number_pc[0] | 'Printing second partition' >> beam.Map(lambda x: print("Second Part: " + str(x))) # beam.Map(print)

p.run()
