import json
import unittest
from unittest.mock import MagicMock
import sys
import os

# Permitir importar desde src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from consumer import TaskConsumer


class TestExample1(unittest.TestCase):
    def test_consumer_processes_message_and_acks(self):
        consumer = TaskConsumer()
        consumer.processing_time = 0  # Evitar demoras en unit tests

        mock_ch = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 42

        payload = {
            "task_id": 1,
            "title": "Tarea de prueba #1",
            "description": "Prueba unitaria"
        }
        body = json.dumps(payload).encode("utf-8")

        consumer.on_message(mock_ch, mock_method, None, body)

        # Debe llamar basic_ack con el tag correcto
        mock_ch.basic_ack.assert_called_once_with(delivery_tag=42)
        self.assertEqual(consumer.processed_count, 1)

    def test_consumer_nacks_on_corrupt_payload(self):
        consumer = TaskConsumer()
        mock_ch = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 99

        # Payload inválido (no es JSON)
        corrupt_body = b"NOT_A_JSON"

        consumer.on_message(mock_ch, mock_method, None, corrupt_body)

        # Debe llamar basic_nack con requeue=True
        mock_ch.basic_nack.assert_called_once_with(delivery_tag=99, requeue=True)


if __name__ == "__main__":
    unittest.main()
