# Import beam library
import apache_beam as beam
import os

# File path
path = '../Apache_Beam_Data/movies_rating.txt'

# Create pipeline pbject
p1 = beam.Pipeline()

movie_gt_4 = (
    # Pass the pipeline object
    p1
    | beam.io.ReadFromText(path, skip_header_lines=1) # Read the file and skip the headers
    | beam.Map(lambda record: record.split(',')) # Split the lines using comma delimiter
    | beam.Filter(lambda record: float(record[2]) > 4) # Get only the movies with rating > 4
    | beam.io.WriteToText('output/result.txt') # Write the output to a text file
)

p1.run()

# Print the file content
print(os.system('cat output/result*'))
