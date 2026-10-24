import json
import unittest
from unittest.mock import MagicMock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from subscriber import NodeSubscriber
from publisher import calculate_hash


class TestExample2(unittest.TestCase):
    def test_subscriber_handles_block_mined_event(self):
        subscriber = NodeSubscriber()
        subscriber.node_id = "test-node-1"

        mock_ch = MagicMock()
        mock_method = MagicMock()

        payload = {
            "event_type": "BLOCK_MINED",
            "block_index": 1,
            "timestamp": "2026-10-14T17:00:00Z",
            "miner": "Miner-Test",
            "previous_hash": "0000",
            "block_hash": "abcd1234ef5678",
            "difficulty": 4,
            "nonce": 1042,
            "transactions_count": 15
        }
        body = json.dumps(payload).encode("utf-8")

        subscriber.on_message(mock_ch, mock_method, None, body)

        self.assertIn(1, subscriber.received_blocks)
        self.assertEqual(len(subscriber.received_blocks), 1)

    def test_calculate_hash_deterministic(self):
        hash1 = calculate_hash(1, "0000", "2026-10-14T17:00:00Z", 100, "Miner-1")
        hash2 = calculate_hash(1, "0000", "2026-10-14T17:00:00Z", 100, "Miner-1")
        hash3 = calculate_hash(2, "0000", "2026-10-14T17:00:00Z", 100, "Miner-1")

        self.assertEqual(hash1, hash2)
        self.assertNotEqual(hash1, hash3)
        self.assertEqual(len(hash1), 64)


if __name__ == "__main__":
    unittest.main()
