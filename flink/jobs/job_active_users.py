from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("bets") \
        .set_group_id("flink-active-users") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=None, source_name="KafkaSource")

    def parse_user(value):
        try:
            data = json.loads(value)
            return (data["user_id"], 1)
        except:
            return ("invalid", 0)

    ds.map(parse_user) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1])) \
        .map(lambda x: ("active_users", 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1])) \
        .print()

    env.execute("Active Users Job")

if __name__ == "__main__":
    main()
