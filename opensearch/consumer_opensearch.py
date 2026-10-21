import json

from confluent_kafka import Consumer
from opensearchpy import OpenSearch
from opensearch_dsl import Search

HOST = "localhost"
PORT = 9200
INDEX_NAME = "epl-standings-index"
INDEX_MAPPING = {
  "mappings": {
    "properties": {
      "team": {"type": "text"},
      "matches_played": {"type": "integer"},
      "points": {"type": "integer"}
    }
  }
}

TOPIC = "epl-standings-topic"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "opensearch-consumer-group"
AUTO_COMMIT_ENABLED = False
AUTO_COMMIT_INTERVAL_MS = 5000
BULK_SIZE = 3
BULK_INDEXING = False

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
        response = client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
        print(response)
    else:
        print("Index already exists:", INDEX_NAME)

    print("Initializing Kafka consumer")
    config = {'bootstrap.servers': BOOTSTRAP_SERVERS,
              'group.id': GROUP_ID,
              'auto.offset.reset': 'latest',
              'enable.auto.commit': AUTO_COMMIT_ENABLED,
              'auto.commit.interval.ms': AUTO_COMMIT_INTERVAL_MS
              }
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    try:
        while True:
            msg_list = consumer.consume(BULK_SIZE, timeout=3)
            #msg = consumer.poll(timeout=3)
            #msg_list = [msg] if msg else []
            msg_count = len(msg_list)

            print(f"Messages consumed:", msg_count)
            if msg_count > 0:
                msg_bulk = []
                for msg in msg_list:
                    if msg.error():
                        print(f"Message error: {msg.error()}")
                    else:
                        print(f"Consumed message:", msg.value())
                        # msg_id = msg.topic() + "_" + str(msg.partition()) + "_" + str(msg.offset())
                        msg_value = msg.value().decode('utf-8')
                        msg_dict = json.loads(msg_value)
                        msg_id = msg_dict['team']

                        if BULK_INDEXING:
                            # Bulk message indexing
                            msg = '{"index": {"_index":"' + INDEX_NAME + '", "_id":"' + msg_id + '"}} \n {' + f'"team": "{msg_dict["team"]}", "matches_played": {msg_dict["matches_played"]}, "points": {msg_dict["points"]}' + '}'
                            print("Collect message for bulk indexing:", msg)
                            msg_bulk.append(msg)
                        else:
                            # Sinle message indexing
                            print("Indexing single message:", msg_value)
                            msg_single = {
                                "team": msg_dict['team'],
                                "matches_played": msg_dict['matches_played'],
                                "points": msg_dict['points']
                            }
                            response = client.index(INDEX_NAME, id=msg_id, body=msg_single)
                            print("Response: ", response)

                if BULK_INDEXING:
                    print("Indexing bulk messages:", msg_bulk)
                    response = client.bulk(index=INDEX_NAME, body=msg_bulk)
                    print("Response:", response)

                if not AUTO_COMMIT_ENABLED:
                    print("Committing message offset manually")
                    consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        print("The application was stopped by the user")
    finally:
        print("Close Kafka consumer and OpenSearch client")
        consumer.close()
        client.close()
