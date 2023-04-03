from flask import Flask, request, json
from google.cloud import datastore
from google.cloud import pubsub_v1

datastore_client = datastore.Client()

app = Flask(__name__)

key = 'utility'
limit = 50

publisher = pubsub_v1.PublisherClient()
topic_path = "projects/event-driven-pipeline/topics/endpoint"


def store_json(data):
    entity = datastore.Entity(key=datastore_client.key('utility'))
    entity.update(data)

    datastore_client.put(entity)


def fetch_data():
    query = datastore_client.query(kind=key)
    query.order = ['-timestamp']

    data = query.fetch(limit=limit)
    return data


@app.route("/", methods=['GET'])
def hello():
    return "Hello, world"


@app.route("/collect", methods=['POST'])
def process_json():
    data = json.loads(request.data)
    store_json(data)

    future = publisher.publish(topic_path, str(data).encode('utf-8'))
    print(f'published message id {future.result()}')
    return data, 201


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8000, debug=True)
