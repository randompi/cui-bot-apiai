
import logging
import json
import requests

logger = logging.getLogger(__name__)

class PersistenceException(Exception):
    pass

class InMemPersister(object):
    def __init__(self):
        self.mem = {}

    def get(self, key):
        return self.mem[key]

    def mget(self, keys):
        key_val_pairs = {}
        for key in keys:
            key_val_pairs[key] = self.mem[key]
        return key_val_pairs

    def set(self, key, value):
        self.mem[key] = value

    def list(self):
        return self.mem.keys()

    def delete(self, key):
        del self.mem[key]

class BeepBoopPersister(object):
    def __init__(self, persist_url, persist_token):
        self.persist_url = persist_url
        self.persist_token = persist_token

    def get(self, key):
        url = '{}/persist/kv/{}'.format(self.persist_url, key)
        logger.debug("get:: url: {}".format(url))
        resp = requests.get(url, headers=self._prepare_headers())
        if resp.status_code == 200:
            return resp.json()
        else:
            raise PersistenceException('Unexpected response: {}'.format(resp.status_code))

    def set(self, key, value):
        if value == None:
            raise PersistenceException('Cannot set a value of None')
        url = '{}/persist/kv/{}'.format(self.persist_url, key)
        logger.debug("set:: url: {}, value: {}".format(url, value))
        resp = requests.put(url, json=json.loads(value))
        if resp.status_code != 200:
            raise PersistenceException('Unexpected response: {}'.format(resp.status_code))


    def _prepare_headers(self):
        return {
            'Accept': 'application/json',
            'Authorization': ('Bearer %s' % self.persist_token)
        }
