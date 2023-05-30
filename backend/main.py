from flask import Flask, json, request
import uuid
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.cosmos import CosmosClient
from azure.messaging.webpubsubservice import WebPubSubServiceClient
app = Flask(__name__)

DATABASE_NAME = "cosmicworks"
CONTAINER_NAME = "data"
client = CosmosClient.from_connection_string("AccountEndpoint=https://endpointcosmos.documents.azure.com:443/;AccountKey=m8LD7A5sWsiAqvfPHkGBkQCepDNebI2iunln8GA8fypznI8aFkgNHCTrr1dSwpoldmVswl3oWO1VACDbv1s84A==;")

database = client.get_database_client(DATABASE_NAME)

container = database.get_container_client(CONTAINER_NAME)

NAMESPACE_CONNECTION_STRING = "Endpoint=sb://endpointbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xccJjKsTNebVapH1MqfOVI5hX+fJh68WN+ASbBTTm/A="
QUEUE_NAME = "endpointbusqueue"
@app.route("/", methods=['GET'])
def hello():
    return "Hello, World", 200

async def send_message(sender, message):
    single_message = ServiceBusMessage(str(message))
    await sender.send_messages(single_message)

@app.route("/collections", methods=['POST'])
def process_json():
    data = json.loads(request.data)
    data["id"] = data["email"]
    container.create_item(data)
    # Send to bus.
    client = ServiceBusClient.from_connection_string(NAMESPACE_CONNECTION_STRING)
    sender = client.get_queue_sender(QUEUE_NAME)
    send_message(sender, data)
    return data, 201

if __name__ == "__main__":
    app.run()
