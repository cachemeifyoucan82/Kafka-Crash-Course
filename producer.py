from confluent_kafka import Producer
import uuid
import json

'''
- this creating new kafka new producer configuation and where kafka is accessible
- boostrap.servers refers to kafka using this address to discover all the brokers in the cluster
- basically if you want to send events you produce you send it to localhost:9092 '''
producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

#callback function that reports if the event was delivered or not
def delivery_report(err, msg):
    if err:
        print(f"Delivery report failed: {err}")
    else:
        print(f"Delivery succeeded: {msg.value().decode("utf-8")}")
        print(dir(msg))

''' 
- create an event we will send to kafka using json object key:value
- kafka relies on uuid for things like cluster IDs, topics, ect.
'''
order = {
    "order_id": str(uuid.uuid4()),
    "user": "nando",
    "item": "chicken",
    "quantity": 1
}

'''
- we have to convert this order into a UTF-8 
'''
value = json.dumps(order).encode("utf-8")

''' 
- Kafak event to send 
- value is sent to kafka and saves this to a topic called orders
- if a topic isn;t created then orders is created 
- callback says if event was delivered or not'''
producer.produce(
    topic="orders", 
    value=value,
    callback=delivery_report)

producer.flush() #makes sure code runs cleaner, all buffer that hasn't been sent gets sent before any other event is sent
