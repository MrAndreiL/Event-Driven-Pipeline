from google.cloud import pubsub_v1
import base64
import ast
import json

publisher = pubsub_v1.PublisherClient()
bill = 0.75
topic_path = "projects/event-driven-pipeline/topics/enriched"


def read_write_pubsub(event, context):
    message = base64.b64decode(event['data']).decode('utf-8')

    message = ast.literal_eval(message)

    response = message.copy()

    response['bill'] = bill * response['watts']

    publisher_future = publisher.publish(topic_path,
                                         json.dumps(response).encode('utf-8'))
    print(f'published message id {publisher_future.result()}')
