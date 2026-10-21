import random
from confluent_kafka import Producer

if __name__ == '__main__':
    print("Kafka Producer")

    config = {'bootstrap.servers': 'localhost:9092'}

    producer = Producer(config)

    topic, value = 'test_topic', f'test_value_{random.randint(0, 1000)}'

    print(f"Producing message to topic: {topic}; value: {value}")
    producer.produce(topic, value)

    producer.flush()
