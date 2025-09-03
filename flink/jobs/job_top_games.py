from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream.window import TumblingProcessingTimeWindows, TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
import json
from typing import Tuple


def main():
    """
    Запускает Flink-задачу для подсчета ставок.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 1. Определение источника (Source) из Kafka
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("bets") \
        .set_group_id("flink-top-games") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 2. Создание DataStream из Kafka-источника
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)).with_timestamp_assigner(lambda event, timestamp: event.get_timestamp())
    ds = env.from_source(
        source,
        # WatermarkStrategy.no_watermarks(),
        watermark_strategy,
        "Kafka Source"
    )

    # 3. Обработка данных
    # Flink обрабатывает данные и преобразует их в JSON-строку.
    # SimpleStringSchema в стоке сам закодирует эту строку в байты.
    processed_stream = ds.map(parse_bet, output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))\
        .reduce(lambda a, b: (a[0], a[1] + b[1])) \
        .map(to_json_string, output_type=Types.STRING())

    # 4. Определение стока (Sink) для вывода в Kafka
    # Возвращаем SimpleStringSchema для сериализации значений.
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("top_games")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # 5. Подключение обработанного потока к стоку
    processed_stream.sink_to(sink)

    # 6. Запуск задачи
    env.execute("Top Games Job")


def parse_bet(value: str) -> Tuple[str, int]:
    """
    Разбирает JSON-строку с данными о ставке.
    Возвращает кортеж (game_id, 1) для подсчета.
    """
    try:
        data = json.loads(value)
        if "game" in data:
            return (data["game"], 1)
    except Exception as e:
        print(f"[ERROR parse_bet] {e} | value={value}")
    return ("invalid", 0)


def to_json_string(record: Tuple[str, int]) -> str:
    """
    Преобразует кортеж (game_id, count) в JSON-строку.
    """
    return json.dumps({"game": record[0], "count": record[1]})


if __name__ == "__main__":
    main()
