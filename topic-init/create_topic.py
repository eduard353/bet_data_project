import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
# Получаем список топиков из переменной окружения, разделённых запятой
TOPIC_NAMES = os.getenv("TOPIC_NAME", "bets,top_games").split(",")

def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id="topic-init"
    )

    topic_list = [NewTopic(name=name.strip(), num_partitions=1, replication_factor=1) for name in TOPIC_NAMES]

    for topic in topic_list:
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"[TOPIC-INIT] Created topic '{topic.name}'")
        except TopicAlreadyExistsError:
            print(f"[TOPIC-INIT] Topic '{topic.name}' already exists")
        except Exception as e:
            print(f"[TOPIC-INIT] Error creating topic '{topic.name}': {e}")

    admin_client.close()

if __name__ == "__main__":
    # ждём пока Kafka поднимется
    time.sleep(10)
    create_topics()
