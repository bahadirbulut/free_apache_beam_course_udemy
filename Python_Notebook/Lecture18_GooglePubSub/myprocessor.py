import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
# from apache_beam import window

serviceAccount = 'bahadir-sandbox-8c3f7daa0302.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

input_subscription = 'projects/bahadir-sandbox/subscriptions/movie_subscription'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

comedy_movies = 'projects/bahadir-sandbox/topics/comedy_movies'

pubsub_pipeline = (
        p
        | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        | 'Split the records by comma' >> beam.Map(lambda row: row.decode("utf-8").split(','))
        | 'Filter movies with comedy genre' >> beam.Filter(lambda row: ("Comedy" in row[2]))
        | 'Converting to byte String' >> beam.Map(lambda row: (''.join(row).encode('utf-8')))
        | 'Publish to output topic' >> beam.io.WriteToPubSub(comedy_movies)
)
result = p.run()
result.wait_until_finish()
