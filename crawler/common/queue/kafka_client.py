from confluent_kafka import Producer, Consumer, KafkaError
import json
import socket
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap_servers=None):
        if bootstrap_servers is None:
            # Lấy từ biến môi trường hoặc mặc định
            bootstrap_servers = os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092"
            )

        self.producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "client.id": socket.gethostname()}
        )

    def send(self, topic, value, key=None):
        """Gửi message tới Kafka topic"""
        try:
            # Nếu value là dict, chuyển đổi thành JSON string
            if isinstance(value, dict):
                try:
                    # Xử lý các giá trị không thể serialize trong dict
                    # Lọc ra các giá trị None và convert dates/datetimes
                    clean_value = self._clean_dict_for_json(value)
                    value = json.dumps(clean_value)
                except TypeError as e:
                    logger.error(f"JSON serialization error: {e}")
                    # Try a more aggressive cleaning approach
                    cleaned = self._aggressive_clean_for_json(value)
                    value = json.dumps(cleaned)
                except Exception as e:
                    logger.error(f"Failed to serialize dict to JSON: {e}")
                    return False

            # Gửi message đến Kafka
            self.producer.produce(
                topic=topic,
                key=key,
                value=value.encode("utf-8") if isinstance(value, str) else value,
            )
            self.producer.flush()
            logger.debug(f"Sent message to topic {topic}")
            return True
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            return False

    def _clean_dict_for_json(self, d):
        """Clean a dictionary to make it JSON serializable"""
        clean = {}
        for k, v in d.items():
            if v is None:
                clean[k] = None
            elif isinstance(v, dict):
                clean[k] = self._clean_dict_for_json(v)
            elif isinstance(v, list):
                clean[k] = [
                    self._clean_dict_for_json(i) if isinstance(i, dict) else i
                    for i in v
                ]
            elif isinstance(v, (datetime, datetime.date)):
                clean[k] = v.isoformat()
            elif hasattr(v, "__dict__"):
                # Convert objects to dictionaries
                clean[k] = self._clean_dict_for_json(v.__dict__)
            else:
                try:
                    # Try to convert the value to something JSON serializable
                    json.dumps(v)
                    clean[k] = v
                except (TypeError, ValueError):
                    # If it can't be serialized, convert to string
                    clean[k] = str(v)
        return clean

    def _aggressive_clean_for_json(self, value):
        """Aggressively clean the object for JSON serialization by converting all complex types to strings"""
        if isinstance(value, dict):
            return {k: self._aggressive_clean_for_json(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._aggressive_clean_for_json(i) for i in value]
        elif isinstance(value, (int, float, bool, str, type(None))):
            return value
        else:
            # Convert anything else to a string
            return str(value)


class KafkaConsumer:
    def __init__(self, topics, group_id, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092"
            )

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 600000,  # 10 phút
            }
        )
        self.consumer.subscribe(topics)
        logger.info(f"Kafka Consumer subscribed to topics: {topics}")

    def commit(self):
        """Commit offset thủ công"""
        try:
            self.consumer.commit()
            return True
        except Exception as e:
            logger.error(f"Error committing offsets: {e}")
            return False

    def consume(self, timeout=1.0):
        """Tiêu thụ message từ Kafka"""
        try:
            msg = self.consumer.poll(timeout)

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                else:
                    logger.error(f"Error while consuming: {msg.error()}")
                return None

            # Decode giá trị message
            try:
                value = msg.value().decode("utf-8")
                return json.loads(value)
            except json.JSONDecodeError:
                return value
            except Exception as e:
                logger.error(f"Error decoding message: {e}")
                return msg.value()

        except Exception as e:
            logger.error(f"Error consuming message from Kafka: {e}")
            return None

    def close(self):
        """Đóng kết nối consumer"""
        self.consumer.close()
