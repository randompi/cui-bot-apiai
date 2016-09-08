# -*- coding: utf-8 -*-

import logging
from mock import patch
import pytest
import sys
import json

import bot.data_models as dm
import bot.event_handler as ev

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger()

def load_json_tests(name):
    # Load file which contains test data
    # TODO: generalize by stripping out filename from fixture name
    with open('tests/fixtures/intent_to_exprs.json') as json_file:
        json_d = json.load(json_file)

    # Tests are each expression and response per intent
    for intent_name, intent in json_d.iteritems():
        print 'intent: {}'.format(intent_name)
        for intent_expr in intent:
            yield intent_expr

def pytest_generate_tests(metafunc):
    """ This allows us to load tests from external files by
    parametrizing tests with each test case found in a data_X
    file """
    for fixture in metafunc.fixturenames:
        if fixture.startswith('json_'):
            # Load associated test data
            tests = load_json_tests(fixture)
            metafunc.parametrize(fixture, tests)

@pytest.fixture(scope="function")
@patch('bot.messenger.Messenger')
def event_handler(mock_msgr):
    slack_clients = None
    msg_writer = mock_msgr
    evt_hndlr = ev.RtmEventHandler(slack_clients, msg_writer)
    return evt_hndlr


def test_all_intent_expressions(json_intent_to_exprs, event_handler):
    resp = json_intent_to_exprs.get('resp')
    assert resp.get('status').get('code') == 200

    logger.debug('Calling _handle_apiai_resp!')
    msg_resp = event_handler._handle_apiai_response(resp, {'channel':'testChannel'})
    logger.debug('msg_resp: {}'.format(msg_resp))
    assert msg_resp is not None
    assert ':x:' not in msg_resp
    assert 'Empty DataFrame' not in msg_resp

