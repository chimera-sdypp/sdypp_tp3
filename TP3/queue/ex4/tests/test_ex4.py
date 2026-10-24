import json
import unittest
from unittest.mock import MagicMock
import sys
import os

# Permitir importar desde src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
import pika
from consumer import RetryConsumer, next_action, retry_headers, RETRY_COUNT_HEADER, DELAY_HEADER, SCHEDULED_AT_HEADER
from dlq_consumer import describe_death
from producer import build_payload
from topology import MAIN_QUEUE, DLX_EXCHANGE, DLQ_ROUTING_KEY, declare_topology, retry_delays, wait_queue_name


def make_delivery(tag, retry_count=None):
    method = MagicMock()
    method.delivery_tag = tag
    props = pika.BasicProperties(
        content_type="application/json",
        headers=None if retry_count is None else {RETRY_COUNT_HEADER: retry_count},
    )
    return MagicMock(), method, props


def make_consumer(fail_probability):
    consumer = RetryConsumer()
    consumer.processing_time = 0
    consumer.fail_probability = fail_probability
    return consumer


class TestBackoff(unittest.TestCase):
    def test_default_delays_are_exponential(self):
        self.assertEqual(retry_delays(), [1, 2, 4, 8])

    def test_delays_follow_base(self):
        self.assertEqual(retry_delays(max_retries=3, base=2), [2, 4, 8])

    def test_next_action_retries_then_dlq(self):
        delays = [1, 2, 4, 8]
        self.assertEqual([next_action(n, delays) for n in range(5)], [
            ("retry", 1), ("retry", 2), ("retry", 4), ("retry", 8), ("dlq", None)
        ])

    def test_retry_headers_increment_and_drop_x_death(self):
        headers = retry_headers({"x-death": [{}], "trace": "abc", RETRY_COUNT_HEADER: 1}, 1, 2, 100.0)
        self.assertEqual(headers, {"trace": "abc", RETRY_COUNT_HEADER: 2, SCHEDULED_AT_HEADER: 100000, DELAY_HEADER: 2})

    def test_retry_headers_are_amqp_encodable(self):
        # pika no puede serializar floats en una tabla AMQP
        headers = retry_headers(None, 0, 1, 1792010324.604628)
        pika.BasicProperties(headers=headers).encode()


class TestTopology(unittest.TestCase):
    def test_declares_one_wait_queue_per_delay(self):
        channel = MagicMock()
        declare_topology(channel)

        for delay in [1, 2, 4, 8]:
            channel.queue_declare.assert_any_call(
                queue=wait_queue_name(delay),
                durable=True,
                arguments={
                    "x-message-ttl": delay * 1000,
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": MAIN_QUEUE,
                },
            )
        channel.queue_declare.assert_any_call(
            queue=MAIN_QUEUE,
            durable=True,
            arguments={"x-dead-letter-exchange": DLX_EXCHANGE, "x-dead-letter-routing-key": DLQ_ROUTING_KEY},
        )


class TestConsumer(unittest.TestCase):
    def test_success_acks_without_retry(self):
        consumer = make_consumer(fail_probability=0.0)
        ch, method, props = make_delivery(1)
        consumer.on_message(ch, method, props, json.dumps({"task_id": 1}).encode())

        ch.basic_ack.assert_called_once_with(delivery_tag=1)
        ch.basic_publish.assert_not_called()

    def test_first_failure_goes_to_1s_wait_queue(self):
        consumer = make_consumer(fail_probability=1.0)
        ch, method, props = make_delivery(2)
        consumer.on_message(ch, method, props, json.dumps({"task_id": 2}).encode())

        kwargs = ch.basic_publish.call_args.kwargs
        self.assertEqual(kwargs["routing_key"], wait_queue_name(1))
        self.assertEqual(kwargs["properties"].headers[RETRY_COUNT_HEADER], 1)
        self.assertTrue(kwargs["mandatory"])
        ch.basic_ack.assert_called_once_with(delivery_tag=2)

    def test_retry_is_published_before_ack(self):
        consumer = make_consumer(fail_probability=1.0)
        ch, method, props = make_delivery(3, retry_count=2)
        consumer.on_message(ch, method, props, json.dumps({"task_id": 3}).encode())

        nombres = [c[0] for c in ch.method_calls]
        self.assertLess(nombres.index("basic_publish"), nombres.index("basic_ack"))
        self.assertEqual(ch.basic_publish.call_args.kwargs["routing_key"], wait_queue_name(4))

    def test_fourth_retry_exhausted_goes_to_dlq(self):
        consumer = make_consumer(fail_probability=1.0)
        ch, method, props = make_delivery(4, retry_count=4)
        consumer.on_message(ch, method, props, json.dumps({"task_id": 4}).encode())

        ch.basic_nack.assert_called_once_with(delivery_tag=4, requeue=False)
        ch.basic_publish.assert_not_called()
        ch.basic_ack.assert_not_called()

    def test_force_fail_ignores_probability(self):
        consumer = make_consumer(fail_probability=0.0)
        ch, method, props = make_delivery(5)
        consumer.on_message(ch, method, props, json.dumps({"task_id": 5, "force_fail": True}).encode())

        self.assertEqual(ch.basic_publish.call_args.kwargs["routing_key"], wait_queue_name(1))

    def test_unroutable_retry_is_requeued(self):
        consumer = make_consumer(fail_probability=1.0)
        ch, method, props = make_delivery(6)
        ch.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        consumer.on_message(ch, method, props, json.dumps({"task_id": 6}).encode())

        ch.basic_nack.assert_called_once_with(delivery_tag=6, requeue=True)
        ch.basic_ack.assert_not_called()

    def test_corrupt_payload_goes_straight_to_dlq(self):
        consumer = make_consumer(fail_probability=0.0)
        ch, method, props = make_delivery(7)
        consumer.on_message(ch, method, props, b"NOT_A_JSON")

        ch.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)

    def test_fail_rate_is_about_half(self):
        consumer = make_consumer(fail_probability=0.5)
        consumer.rng.seed(42)
        fallos = 0
        for _ in range(2000):
            try:
                consumer.process({})
            except RuntimeError:
                fallos += 1
        self.assertAlmostEqual(fallos / 2000, 0.5, delta=0.05)


class TestProducerAndDlq(unittest.TestCase):
    def test_force_fail_every_nth(self):
        self.assertEqual([i for i in range(1, 11) if build_payload(i, 5)["force_fail"]], [5, 10])

    def test_dlq_shows_retry_count(self):
        props = MagicMock()
        props.headers = {RETRY_COUNT_HEADER: 4, "x-death": [{"reason": "rejected", "queue": MAIN_QUEUE, "count": 1}]}
        self.assertIn("reintentos=4", describe_death(props))


if __name__ == "__main__":
    unittest.main()
