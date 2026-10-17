from confluent_kafka import Consumer
from opensearchpy import OpenSearch
from opensearch_dsl import Search

HOST = "localhost"
PORT = 9200
INDEX_NAME = "my-index"

BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "opensearch-consumer-group"
TOPIC = "my-topic"
BULK_SIZE = 1000

if __name__ == "__main__":
    client = OpenSearch(
        [{"host": HOST, "port": PORT}],
        http_compress=True,  # enables gzip compression for request bodies
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    index_exists = client.indices.exists(index=INDEX_NAME)

    if not index_exists:
        print("Creating index:", INDEX_NAME)
        response = client.indices.create(index=INDEX_NAME)
        print(response)
    else:
        print("Index already exists:", INDEX_NAME)

    print("Initializing Kafka consumer")
    config = {'bootstrap.servers': BOOTSTRAP_SERVERS,
              'group.id': GROUP_ID,
              'auto.offset.reset': 'latest',
              'enable.auto.commit': False,
              'auto.commit.interval.ms': 5000
              }
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    try:

        while True:
            msg_list = consumer.consume(BULK_SIZE, timeout=3)

            print(f"Messages consumed:", len(msg_list))
            msg_bulk = ""
            for msg in msg_list:
                if msg.error():
                    print(f"Message error: {msg.error()}")
                else:
                    msg_id = msg.topic() + "_" + str(msg.partition()) + "_" + str(msg.offset())
                    msg_value = msg.value().decode('utf-8')

                    print(f"Message:", msg_value)

                    # print("Indexing message in OpenSearch:")
                    # response = client.index(INDEX_NAME, id=msg_id, body={'value': msg_value})
                    # print(response)
                    msg_bulk += f'{{"value": "{msg_value}"}}\n'

            if len(msg_bulk) > 0:
                print("Bulk indexing messages:", msg_bulk)
                client.bulk(index=INDEX_NAME, body=msg_bulk)

                if not config.get('enable.auto.commit', False):
                    print("Committing message offset manually")
                    consumer.commit(asynchronous=False)
    finally:
        consumer.close()
