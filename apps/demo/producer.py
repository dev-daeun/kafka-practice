import random
import time
from sys import stdout

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    IntegerSerializer,
    StringSerializer,
    IntegerDeserializer,
    StringDeserializer,
)

from demo.common import BASE_KAFKA_CONFIG, TOPIC_NAME


producer = Producer(BASE_KAFKA_CONFIG)

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
                topic=TOPIC_NAME,
                key=integer_serializer(key),
                value=string_serializer(f"this-is-{key}"),
                partition=partitioner(key),
                on_delivery=delivery_report,
            )
            time.sleep(3)
    finally:
        producer.flush()


produce()
