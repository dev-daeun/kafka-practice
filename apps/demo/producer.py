import random
from sys import stdout

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    IntegerSerializer,
    StringSerializer,
    IntegerDeserializer,
    StringDeserializer,
)
from dotenv import dotenv_values

kafka_config: dict = dotenv_values(".env.kafka")

topic_name = "first-topic"

# config key/value : https://github.com/confluentinc/librdkafka/blob/v2.2.0/CONFIGURATION.md 참고
producer = Producer(
    {
        "bootstrap.servers": "cluster.playground.cdkt.io:9092",
        "security.protocol": "sasl_ssl",
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
        "sasl.mechanism": "PLAIN",
    }
)


integer_serializer = IntegerSerializer()
integer_deserializer = IntegerDeserializer()
string_serializer = StringSerializer()
string_deserializer = StringDeserializer()


def delivery_report(err, msg):
    if err is not None:
        stdout.write(
            f"Record {integer_deserializer(msg.key())}:{string_deserializer(msg.value())} "
            f"failed to be delivered: {err}\n"
        )
        return
    stdout.write(
        f"Record {integer_deserializer(msg.key())}:{string_deserializer(msg.value())} "
        f"successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}\n"
    )
    """
    Record 3:this-is-3 successfully produced to first-topic [0] at offset 20039
    Record 6:this-is-6 successfully produced to first-topic [0] at offset 20040
    Record 9:this-is-9 successfully produced to first-topic [0] at offset 20041
    Record 8:this-is-8 successfully produced to first-topic [2] at offset 33519
    Record 7:this-is-7 successfully produced to first-topic [1] at offset 20045
    Record 9:this-is-9 successfully produced to first-topic [0] at offset 20042
    Record 5:this-is-5 successfully produced to first-topic [2] at offset 33520
    Record 3:this-is-3 successfully produced to first-topic [0] at offset 20043

    """


def partitioner(key: int):
    # return key % len(producer.list_topics().topics[TOPIC_NAME].partitions)
    return key % 3


def produce():
    try:
        while True:
            producer.poll(5)
            key = random.choice(range(1, 10))
            producer.produce(
                topic=topic_name,
                key=integer_serializer(key),
                value=string_serializer(f"this-is-{key}"),
                partition=partitioner(key),
                on_delivery=delivery_report,
            )
    finally:
        producer.flush()


produce()
