# -*- coding: utf-8 -*-

import mock
import unittest
import sys
import os
import json

import bot.memory as memory


class TestMemoryInMemPersister(unittest.TestCase):
    def setUp(self):
        self.memory = memory.InMemPersister()

    def tearDown(self):
        del self.memory

    def test_getUnkKey(self):
        key = 'unk'
        self.assertRaises(KeyError, self.memory.get, key)

    def test_setThenGet(self):
        key = 'test-key'
        val = 'test-value'
        self.memory.set(key, val)
        retval = self.memory.get(key)
        self.assertEqual(val, retval)

class TestMemoryBeepBoopPersister(unittest.TestCase):

    def setUp(self):
        self.persist_url = "http://test-persist"
        self.persist_token = "TEST-TOKEN"
        self.memory = memory.BeepBoopPersister(self.persist_url, self.persist_token)

    @mock.patch('bot.memory.requests.get')
    def test_getKnownKeyStringValue(self, mock_get):
        mock_response = mock.Mock()
        expected_val = "test-value"
        val_json = json.dumps(expected_val)
        expected_dict = {
            "value" : val_json
        }
        mock_response.status_code = 200
        mock_response.json.return_value = expected_dict
        mock_get.return_value = mock_response

        key = "known-key"
        response_val = self.memory.get(key)

        url = "{}/persist/kv/{}".format(self.persist_url, key)
        mock_get.assert_called_with(url, headers=self.memory._prepare_headers())
        self.assertEqual(1, mock_response.json.call_count)

        self.assertEqual(response_val, expected_val)


if __name__ == '__main__':
    unittest.main()
