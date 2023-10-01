from dotenv import dotenv_values

TOPIC_NAME = "first-topic"

_KAFKA_CONFIG: dict = dotenv_values("../.env.kafka")

# config key/value : https://github.com/confluentinc/librdkafka/blob/v2.2.0/CONFIGURATION.md 참고
BASE_KAFKA_CONFIG = {
    "bootstrap.servers": _KAFKA_CONFIG["bootstrap-server"],
    "security.protocol": _KAFKA_CONFIG["security-protocol"],
    "sasl.username": _KAFKA_CONFIG["username"],
    "sasl.password": _KAFKA_CONFIG["password"],
    "sasl.mechanism": "PLAIN",
}
