import random
from time import sleep

from confluent_kafka import Producer

if __name__ == '__main__':
    print("Kafka Producer with callback")

    config = {'bootstrap.servers': 'localhost:9092',
              'acks': 'all',
              # 'min.insync.replicas': 2, # it's a broker side configuration
              'enable.idempotence': True,
              "retries": 1048576,
              "delivery.timeout.ms": 120000,
              'batch.size': 16384,  # 16KB
              'linger.ms': 10,
              'compression.type': 'snappy'
              }

    producer = Producer(config)

    print("Producer config: ", producer)

    def _callback(err, msg):
        if err:
            print(f"Failed to deliver message: {err.str()}")
        else:
            print(f"Message delivered: \n"
                  f"- topic: {msg.topic()} \n"
                  f"- value: {msg.value()} \n"                  
                  f"- partition: {msg.partition()} \n"
                  f"- offset: {msg.offset()} \n")

    for i in range(1):
        for j in range(1):
            topic, value = 'test_topic', f'test_value_{random.randint(0, 1000)}'

            print(f"Producing message: topic - {topic}; value - {value}")
            producer.produce(topic, value, on_delivery=_callback)
        sleep(.005)

    producer.flush()
