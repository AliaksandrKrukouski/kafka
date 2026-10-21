import random
from confluent_kafka import Consumer

if __name__ == '__main__':
    print("Kafka Consumer")

    topic = 'test_topic'
    group_id = 'test_group'

    config = {'bootstrap.servers': 'localhost:9092',
              'group.id': group_id,
              'auto.offset.reset': 'earliest'}

    consumer = Consumer(config)
    consumer.subscribe([topic])

    print(f"Consuming messages from topic: {topic}")
    try:
        msg_number = 1
        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("No messages")
            elif msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                print(f"Consumed message #{msg_number}: \n"
                      f"- topic: {msg.topic()} \n"
                      f"- value: {msg.value()} \n"
                      f"- partition: {msg.partition()} \n"
                      f"- offset: {msg.offset()}")
                msg_number += 1

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
