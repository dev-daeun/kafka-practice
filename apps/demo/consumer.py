from confluent_kafka import Consumer, Message, KafkaError
from confluent_kafka.serialization import IntegerDeserializer, StringDeserializer

from demo.common import BASE_KAFKA_CONFIG, TOPIC_NAME

consumer = Consumer(
    **BASE_KAFKA_CONFIG,
    **{
        "group.id": "first-consumer-group",
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    },
)
integer_deserializer = IntegerDeserializer()
string_deserializer = StringDeserializer()


def process_msg(msg: Message):
    print(
        f"message in [{msg.partition()}]: "
        f"key={integer_deserializer(msg.key())} value={string_deserializer(msg.value())}"
    )


def consume_first_topic():
    try:
        consumer.subscribe([TOPIC_NAME])
        while True:
            print("polling...")
            #  If no records are received before this timeout expires, then poll() will return an empty record set.
            msg: Message = consumer.poll(timeout=5)
            if not msg:
                print("No message...")
                continue
            if err := msg.error():
                if err.code() == KafkaError._PARTITION_EOF:
                    print(
                        f"{TOPIC_NAME} [{msg.partition()}] reached end at offset {msg.offset()}"
                    )
                else:
                    print(f"some error occurred while consuming: {err!r}")
            else:
                process_msg(msg)

    finally:
        consumer.close()


consume_first_topic()
"""
polling...
message in [2]: key=8 value=this-is-8
polling...
message in [1]: key=7 value=this-is-7
polling...
message in [1]: key=7 value=this-is-7
polling...
message in [1]: key=7 value=this-is-7
polling...
message in [1]: key=1 value=this-is-1
polling...
message in [0]: key=6 value=this-is-6
polling...
message in [0]: key=9 value=this-is-9
polling...
message in [2]: key=2 value=this-is-2
polling...
message in [0]: key=6 value=this-is-6
polling...
message in [1]: key=7 value=this-is-7
polling...
message in [0]: key=3 value=this-is-3
polling...
message in [0]: key=3 value=this-is-3
polling...
message in [0]: key=3 value=this-is-3
polling...

"""
