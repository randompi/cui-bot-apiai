
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

    def list(self, begins_with=None):
        if begins_with is not None:
            keys = self.mem.keys()
            filtered = [k for k in keys if k.startswith(begins_with)]
            return filtered
        else:
            return self.mem.keys()

    def delete(self, key):
        del self.mem[key]

class BeepBoopPersister(object):
    def __init__(self, persist_url, persist_token):
        self.persist_url = persist_url
        self.persist_token = persist_token

    def get(self, key):
        url = '{}/persist/kv/{}'.format(self.persist_url, key)
        logger.info("get:: url: {}".format(url))
        resp = requests.get(url, headers=self._prepare_headers())
        if resp.status_code == 200:
            return self._unmarshal(resp.text)
        else:
            logger.info('Unexpected response from get:: {}\ntext: {}'.format(resp, resp.text))
            raise PersistenceException('Unexpected response: {}\ntext: {}'.format(resp, resp.text))

    def set(self, key, value):
        if value == None:
            raise PersistenceException('Cannot set a value of None')
        url = '{}/persist/kv/{}'.format(self.persist_url, key)
        value = {'value': value}
        logger.info("set:: url: {}, value: {}".format(url, value))
        resp = requests.put(url, headers=self._prepare_headers(), json=value)
        if resp.status_code != 200:
            logger.info('Unexpected response from set:: {}\ntext: {}'.format(resp, resp.text))
            raise PersistenceException('Unexpected response: {}\ntext: {}'.format(resp, resp.text))

    def list(self, begins_with=None):
        url = '{}/persist/kv'.format(self.persist_url)
        logger.info("list:: url: {}, begins_with: {}".format(url, begins_with))
        params = {}
        if begins_with is not None:
            params = {'begins':begins_with}
        resp = requests.get(url, headers=self._prepare_headers(), params=params)
        if resp.status_code == 200:
            return self._unmarshal(resp.text)
        else:
            logger.info('Unexpected response from list:: {}\ntext: {}'.format(resp, resp.text))
            raise PersistenceException('Unexpected response: {}\ntext: {}'.format(resp, resp.text))

    def delete(self, key):
        url = '{}/persist/kv/{}'.format(self.persist_url, key)
        logger.info("delete:: url: {}".format(url))
        resp = requests.delete(url, headers=self._prepare_headers())
        if resp.status_code == 200:
            return
        else:
            logger.info('Unexpected response from delete:: {}\ntext: {}'.format(resp, resp.text))
            raise PersistenceException('Unexpected response: {}\ntext: {}'.format(resp, resp.text))

    def _marshal(self, val):
        return json.dumps(val)

    def _unmarshal(self, val):
        return json.loads(val)

    def _prepare_headers(self):
        return {
            'Accept': 'application/json',
            'Authorization': ('Bearer %s' % self.persist_token)
        }
