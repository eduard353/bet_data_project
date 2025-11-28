from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.connectors.file_system import OutputFileConfig
from pyflink.datastream.functions import ProcessWindowFunction
from typing import Iterable
import json
import os


def main():
    # 1. Настройка окружения Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    # 2. Получение переменных окружения из .env
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    topic_name = os.getenv("TOPIC_NAME", "bets")
    s3_bucket = os.getenv("S3_BUCKET", "bets")
    active_users_topic = os.getenv("ACTIVE_USERS_TOPIC", "active_users_results")

    # 3. Kafka Source (читаем топик из переменной окружения)
    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_broker) \
        .set_topics(topic_name) \
        .set_group_id("flink-active-users") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")

    # 4. Функция парсинга сообщений
    def parse_user(value):
        try:
            data = json.loads(value)
            return (data["user_id"], 1)
        except Exception:
            return ("invalid", 0)

    # 5. Основная логика: считаем активных пользователей в окне
    active_users = (
        ds.map(parse_user)
          .filter(lambda x: x[0] != "invalid")
          .key_by(lambda x: x[0])
          .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
          .reduce(lambda a, b: (a[0], a[1] + b[1]))
          .map(lambda x: ("active_users", 1))
          .key_by(lambda x: x[0])
          .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
          .process(CountUsersWindowFunction(), output_type=Types.STRING())
    )

    active_users.print()


    # 6. S3 Sink — сохраняем результаты в JSON
    output_path = f"s3://{s3_bucket}/flink/active_users/"

    output_config = OutputFileConfig.builder() \
        .with_part_prefix("active_users") \
        .with_part_suffix(".json") \
        .build()

    s3_sink = FileSink.for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(output_config) \
        .build()

    active_users.sink_to(s3_sink)

    # 7. Kafka Sink — отправляем тот же результат в другой топик
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_broker) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(active_users_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    active_users.sink_to(kafka_sink)

    # 8. Запуск задачи
    env.execute("Active Users → Kafka + S3")


# 9. Функция окна (добавляет время окна)
class CountUsersWindowFunction(ProcessWindowFunction):

    def process(self, key, context: ProcessWindowFunction.Context, elements: Iterable[tuple], out):
        count = sum(e[1] for e in elements)
        window_start = context.window().start
        window_end = context.window().end

        result = {
            "window_start": context.timestamp_to_date_time(window_start).isoformat(),
            "window_end": context.timestamp_to_date_time(window_end).isoformat(),
            "active_users": count
        }

        # Отправляем как JSON-строку
        out.collect(json.dumps(result))


if __name__ == "__main__":
    main()
