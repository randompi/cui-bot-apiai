
import logging
import requests

logger = logging.getLogger(__name__)


class ApiaiDevClient(object):
    def __init__(self, dev_access_token):
        self.dev_access_token = dev_access_token
        self.base_url = 'https://api.api.ai/v1'

    def get_entities(self):
        """Api.ai request to upload entries for an entity.
            See: https://docs.api.ai/docs/entities#get-entities"""
        url = '{}/entities'.format(self.base_url)
        logger.debug("getEntities:: url: {}".format(url))
        resp = requests.get(url, headers=self._prepare_headers())

        return resp

    def post_entry(self, entity_id, entry):
        """Api.ai request to upload entries for an entity.
            See: https://docs.api.ai/docs/entities#post-entitieseidentries"""
        url = '{}/entities/{}/entries'.format(self.base_url, entity_id)
        logger.debug("postEntryToEntity:: entity_id: {}, entry: {}, url: {}".format(entity_id, entry, url))
        resp = requests.post(url, json=entry, headers=self._prepare_headers())

        return resp

    def _prepare_headers(self):
        return {
            'Accept': 'application/json',
            'Authorization': ('Bearer %s' % self.dev_access_token)
        }


class ApiaiResponseParser(object):
    def __init__(self, response):
        self.resp = response