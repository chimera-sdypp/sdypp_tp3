import json
import unittest
from unittest.mock import MagicMock
import sys
import os

# Permitir importar desde src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from consumer import TaskConsumer
from dlq_consumer import DeadLetterConsumer, describe_death
from producer import build_payload
from topology import MAIN_QUEUE, DLX_EXCHANGE, DLQ_QUEUE, DLQ_ROUTING_KEY, declare_topology


def make_delivery(tag):
    method = MagicMock()
    method.delivery_tag = tag
    return MagicMock(), method


class TestTopology(unittest.TestCase):
    def test_main_queue_points_to_dlx(self):
        channel = MagicMock()
        declare_topology(channel)

        channel.exchange_declare.assert_called_once_with(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)
        channel.queue_bind.assert_called_once_with(queue=DLQ_QUEUE, exchange=DLX_EXCHANGE, routing_key=DLQ_ROUTING_KEY)
        channel.queue_declare.assert_any_call(
            queue=MAIN_QUEUE,
            durable=True,
            arguments={"x-dead-letter-exchange": DLX_EXCHANGE, "x-dead-letter-routing-key": DLQ_ROUTING_KEY},
        )


class TestProducer(unittest.TestCase):
    def test_marks_every_nth_task_as_error(self):
        errores = [i for i in range(1, 11) if build_payload(i, error_every=3)["error"]]
        self.assertEqual(errores, [3, 6, 9])

    def test_error_every_zero_disables_errors(self):
        self.assertFalse(any(build_payload(i, error_every=0)["error"] for i in range(1, 11)))


class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.consumer = TaskConsumer()
        self.consumer.processing_time = 0

    def test_acks_valid_message(self):
        ch, method = make_delivery(1)
        self.consumer.on_message(ch, method, None, json.dumps({"task_id": 1, "error": False}).encode())

        ch.basic_ack.assert_called_once_with(delivery_tag=1)
        ch.basic_nack.assert_not_called()

    def test_nacks_without_requeue_when_error_true(self):
        ch, method = make_delivery(2)
        self.consumer.on_message(ch, method, None, json.dumps({"task_id": 3, "error": True}).encode())

        ch.basic_nack.assert_called_once_with(delivery_tag=2, requeue=False)
        ch.basic_ack.assert_not_called()
        self.assertEqual(self.consumer.rejected_count, 1)

    def test_error_must_be_boolean_true(self):
        # "error": "true" (string) no cumple la condición del enunciado
        ch, method = make_delivery(3)
        self.consumer.on_message(ch, method, None, json.dumps({"task_id": 4, "error": "true"}).encode())

        ch.basic_ack.assert_called_once_with(delivery_tag=3)

    def test_corrupt_payload_goes_to_dlq(self):
        ch, method = make_delivery(4)
        self.consumer.on_message(ch, method, None, b"NOT_A_JSON")

        ch.basic_nack.assert_called_once_with(delivery_tag=4, requeue=False)


class TestDeadLetterConsumer(unittest.TestCase):
    def test_describes_x_death_header(self):
        props = MagicMock()
        props.headers = {"x-death": [{"reason": "rejected", "queue": MAIN_QUEUE, "count": 1}]}
        self.assertEqual(describe_death(props), f"reason=rejected queue={MAIN_QUEUE} count=1")

    def test_handles_missing_headers(self):
        props = MagicMock()
        props.headers = None
        self.assertEqual(describe_death(props), "sin header x-death")

    def test_acks_dead_letter(self):
        consumer = DeadLetterConsumer()
        ch, method = make_delivery(7)
        consumer.on_message(ch, method, None, b'{"task_id": 3, "error": true}')

        ch.basic_ack.assert_called_once_with(delivery_tag=7)
        self.assertEqual(consumer.received_count, 1)


if __name__ == "__main__":
    unittest.main()
