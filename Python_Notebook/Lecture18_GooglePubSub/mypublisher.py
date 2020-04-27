# import csv
import time
from google.cloud import pubsub_v1
import os

project = 'bahadir-sandbox'
pubsub_topic = 'projects/bahadir-sandbox/topics/movies'
serviceAccount = 'bahadir-sandbox-8c3f7daa0302.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

input_file = 'ml-latest/movies.csv'

publisher = pubsub_v1.PublisherClient()

with open(input_file, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(pubsub_topic, row)
        time.sleep(1)
