'''
- first we will connect to kakfa broker to subscribe to topic
- listen to message
-  process each message one by one
'''

from confluent_kafka import Consumer
import json

'''
- identifies a group of consumers that are instances of the same program
- auto.offset.reset tells consumer what to to if it can't find where it last left of on reading messages
- if you don't know where you left off on messages start with earlist
'''
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
     "auto.offset.reset": "earliest" 
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Consumer is running and subscribed to orders topic")

'''
- logic tells consumer to check whether there is a new event in the topic where it is described to
- .poll() pings kafka if there is a new event i am subscribed to
'''
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error: ",msg.error())
        continue

#new event a consumer hasn't read yet
    value = msg.value().decode("utf-8")
    order = json.loads(value)

    print(f"Received Order: {order['quanity']} x {order['item']} from {order['user']}")

#kafka is designed to pull any the broker if there are any new messages instead of Kafka pushes any messages
#polling lets consumers to control how and when many times they read those events and messages from different topics good for load balancing
#subscribing to topics means that consumer is telling kafka i want to read from this specific topic but the consumer must pull to receive any events

#logic to shut down gracefullt
producer.flush()