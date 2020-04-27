from google.cloud import pubsub_v1
import time
import os

serviceAccount = 'bahadir-sandbox-8c3f7daa0302.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

subscription_path = 'projects/bahadir-sandbox/subscriptions/comedy_movies_subscription'
subscriber = pubsub_v1.SubscriberClient()


def callback(message):
    print(('Received Message: {}'.format(message)))
    message.ack()


subscriber.subscribe(subscription_path, callback=callback)

while True:
    time.sleep(5)
