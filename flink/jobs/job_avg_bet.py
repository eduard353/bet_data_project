from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("bets") \
        .set_group_id("flink-avg-bet") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=None, source_name="KafkaSource")

    def parse_amount(value):
        try:
            data = json.loads(value)
            return (1, float(data["amount"]))
        except:
            return (0, 0.0)

    ds.map(parse_amount) \
        .key_by(lambda x: 0) \
        .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .map(lambda x: ("avg_bet", x[1] / x[0] if x[0] > 0 else 0)) \
        .print()

    env.execute("Average Bet Job")

if __name__ == "__main__":
    main()
