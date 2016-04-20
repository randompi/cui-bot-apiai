import json
import logging
import re
from wit import Wit

# TODO: make this configurable
access_token = '3ASGKNIRJ4U6PDQ66SYNLYCSLEQIGZWD'

logger = logging.getLogger(__name__)


def first_entity_value(entities, entity):
    if entity not in entities:
        return None
    val = entities[entity][0]['value']
    if not val:
        return None
    return val['value'] if isinstance(val, dict) else val


class RtmEventHandler(object):
    def __init__(self, slack_clients, msg_writer):
        self.clients = slack_clients
        self.msg_writer = msg_writer
        actions = {
            'say': self.say,
            'merge': self.merge,
            'error': self.error,
        }
        self.wit_client = Wit(access_token, actions)

    def say(self, session_id, context, msg):
        logger.debug('say:: session_id: {}, msg: {}'.format(session_id, msg))
        channel = session_id.split(':')[0]
        self.msg_writer.send_message(channel, msg)

    def merge(self, session_id, context, entities, msg):
        logger.debug('merge:: session_id: {}, context: {}, entities:{}, msg:{}'.format(session_id, context, entities, msg))
        hrt_segs = first_entity_value(entities, 'heart_segments')
        if hrt_segs:
            context['heart_segments'] = hrt_segs
        return context

    def error(self, session_id, context, e):
        logger.debug('error:: session_id: {}, context: {}, e: {}'.format(session_id, context, e))
        print(str(e))

    def handle(self, event):

        if 'type' in event:
            self._handle_by_type(event['type'], event)

    def _handle_by_type(self, event_type, event):
        # See https://api.slack.com/rtm for a full list of events
        if event_type == 'error':
            # error
            self.msg_writer.write_error(event['channel'], json.dumps(event))
        elif event_type == 'message':
            # message was sent to channel
            self._handle_message(event)
        elif event_type == 'channel_joined':
            # you joined a channel
            self.msg_writer.write_help_message(event['channel'])
        elif event_type == 'group_joined':
            # you joined a private group
            self.msg_writer.write_help_message(event['channel'])
        else:
            pass

    def _handle_message(self, event):
        # Filter out messages from the bot itself
        if not self.clients.is_message_from_me(event['user']):

            msg_txt = event['text']

            if self.clients.is_bot_mention(msg_txt) or self._is_direct_message(event['channel']):
                # e.g. user typed: "@pybot tell me a joke!"
                if 'help' in msg_txt:
                    self.msg_writer.write_help_message(event['channel'])
                elif re.search('hi|hey|hello|howdy', msg_txt):
                    self.msg_writer.write_greeting(event['channel'], event['user'])
                elif 'joke' in msg_txt:
                    self.msg_writer.write_joke(event['channel'])
                elif 'attachment' in msg_txt:
                    self.msg_writer.demo_attachment(event['channel'])
                else:
                    if msg_txt.startswith('<@U'):
                        # message starts with something like: `<@U11T7N1U3>: `... (so strip off user mention)
                        msg_txt = msg_txt[14:len(msg_txt)]
                    session_id = event['channel'] + ":" + event['user']
                    logger.debug('Sending message: {} to wit_client actions for session_id: {}'.format(msg_txt, session_id))
                    self.wit_client.run_actions(session_id, msg_txt, {})

    def _is_direct_message(self, channel_id):
        return channel_id.startswith('D')
