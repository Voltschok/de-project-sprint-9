import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from lib.redis import RedisClient


class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def __init__(self) -> None:

        self.kafka_host = str(os.getenv('KAFKA_HOST') or "")
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')) or 0)
        self.kafka_consumer_username = str(os.getenv('KAFKA_USER') or "")
        self.kafka_consumer_password = str(os.getenv('KAFKA_PASSWORD') or "")
        self.kafka_consumer_group = str(os.getenv('KAFKA_GROUP') or "")
        self.kafka_consumer_topic = str(os.getenv('KAFKA_SOURCE_TOPIC') or "")
        self.kafka_producer_username = str(os.getenv('KAFKA_USER') or "")
        self.kafka_producer_password = str(os.getenv('KAFKA_PASSWORD') or "")
        self.kafka_producer_topic = str(os.getenv('KAFKA_STG_SERVICE_ORDERS_TOPIC') or "")

        self.redis_host = str(os.getenv('REDIS_HOST') or "")
        self.redis_port = int(str(os.getenv('REDIS_PORT')) or 0)
        self.redis_password = str(os.getenv('REDIS_PASSWORD') or "")

        self.pg_warehouse_host = str(os.getenv('POSTGRES_HOST') or "")
        self.pg_warehouse_port = int(str(os.getenv('POSTGRES_PORT') or 0))
        self.pg_warehouse_dbname = str(os.getenv('POSTGRES_DB_NAME') or "")
        self.pg_warehouse_user = str(os.getenv('POSTGRES_USERNAME') or "")
        self.pg_warehouse_password = str(os.getenv('POSTGRES_PASSWORD') or "")

    def kafka_producer(self):
        return KafkaProducer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_producer_username,
            self.kafka_producer_password,
            self.kafka_producer_topic,
            self.CERTIFICATE_PATH
        )

    def kafka_consumer(self):
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    def redis_client(self) -> RedisClient:
        return RedisClient(
            self.redis_host,
            self.redis_port,
            self.redis_password,
            self.CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )
