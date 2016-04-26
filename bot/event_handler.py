import json
import logging
import re
import traceback
from wit import Wit

# TODO: make this configurable
# access_token = '3ASGKNIRJ4U6PDQ66SYNLYCSLEQIGZWD' # kcherniwchan / My First App
access_token = 'T524XQBBYKKUSVTKG4US7B5KXJJINJHY' # randompi / cui-test


# TODO: Read these in from editable files
# 'value': , 'sd': , 'range_lower': , 'range_upper':
abs_norm_values = {
    'male':{
        'lt35':{
            'lv':{
                'edv':{'value': 173, 'sd': 29, 'range_lower': 115, 'range_upper': 231},
                'esv':{'value': -1, 'sd': 15, 'range_lower': 27, 'range_upper': 87},
                'sv':{'value': 118, 'sd': 18, 'range_lower': 82, 'range_upper': 154},
                'ef':{'value': 67, 'sd': 5, 'range_lower': 57, 'range_upper': 77},
                'mass':{'value': 131, 'sd': 21, 'range_lower': 89, 'range_upper': 173},
            },
            'rv': {
                'edv': {'value': 203, 'sd': 33, 'range_lower': 137, 'range_upper': 269},
                'esv': {'value': 87, 'sd': 20, 'range_lower': 47, 'range_upper': 127},
                'sv': {'value': 116, 'sd': 19, 'range_lower': 78, 'range_upper': 154},
                'ef': {'value': 57, 'sd': 5, 'range_lower': 47, 'range_upper': 67},
                'mass': {'value': 42, 'sd': 8, 'range_lower': 26, 'range_upper': 58},
            },
        },
        'gte35': {
            'lv': {
                'edv': {'value': 149, 'sd': 25, 'range_lower': 99, 'range_upper': 199},
                'esv': {'value': 43, 'sd': 13, 'range_lower': 17, 'range_upper': 69},
                'sv': {'value': 106, 'sd': 19, 'range_lower': 68, 'range_upper': 144},
                'ef': {'value': 71, 'sd': 6, 'range_lower': 59, 'range_upper': 83},
                'mass': {'value': 120, 'sd': 23, 'range_lower': 74, 'range_upper': 166},
            },
            'rv': {
                'edv': {'value': 181, 'sd': 28, 'range_lower': 125, 'range_upper': 237},
                'esv': {'value': 71, 'sd': 17, 'range_lower': 37, 'range_upper': 105},
                'sv': {'value': 110, 'sd': 18, 'range_lower': 74, 'range_upper': 146},
                'ef': {'value': 61, 'sd': 6, 'range_lower': 49, 'range_upper': 73},
                'mass': {'value': 39, 'sd': 7, 'range_lower': 25, 'range_upper': 53},
            },
        },
    },
    'female': {
        'lt35': {
            'lv': {
                'edv': {'value': 137, 'sd': 25, 'range_lower': 87, 'range_upper': 187},
                'esv': {'value': 43, 'sd': 11, 'range_lower': 21, 'range_upper': 65},
                'sv': {'value': 96, 'sd': 18, 'range_lower': 60, 'range_upper': 132},
                'ef': {'value': 69, 'sd': 6, 'range_lower': 57, 'range_upper': 81},
                'mass': {'value': 92, 'sd': 20, 'range_lower': 52, 'range_upper': 132},
            },
            'rv': {
                'edv': {'value': 152, 'sd': 27, 'range_lower': 98, 'range_upper': 206},
                'esv': {'value': 59, 'sd': 12, 'range_lower': 35, 'range_upper': 83},
                'sv': {'value': 93, 'sd': 17, 'range_lower': 59, 'range_upper': 127},
                'ef': {'value': 61, 'sd': 3, 'range_lower': 55, 'range_upper': 67},
                'mass': {'value': 36, 'sd': 7, 'range_lower': 22, 'range_upper': 50},
            },
        },
        'gte35': {
            'lv': {
                'edv': {'value': 128, 'sd': 23, 'range_lower': 82, 'range_upper': 174},
                'esv': {'value': 40, 'sd': 12, 'range_lower': 16, 'range_upper': 64},
                'sv': {'value': 89, 'sd': 16, 'range_lower': 57, 'range_upper': 121},
                'ef': {'value': 69, 'sd': 6, 'range_lower': 57, 'range_upper': 81},
                'mass': {'value': 92, 'sd': 19, 'range_lower': 54, 'range_upper': 130},
            },
            'rv': {
                'edv': {'value': 140, 'sd': 37, 'range_lower': 66, 'range_upper': 214},
                'esv': {'value': 52, 'sd': 22, 'range_lower': 8, 'range_upper': 96},
                'sv': {'value': 93, 'sd': 17, 'range_lower': 50, 'range_upper': 126},
                'ef': {'value': 64, 'sd': 7, 'range_lower': 50, 'range_upper': 78},
                'mass': {'value': 33, 'sd': 7, 'range_lower': 19, 'range_upper': 47},
            },
        },
    },
    'units':{
        'edv': 'ml',
        'esv': 'ml',
        'sv': 'ml',
        'ef': '%',
        'mass': 'g',
        'bsa': 'm^2',
    },
}


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
            'clearContext': self.clearContext,
            'lookupHCV': self.lookupHCV,
            'lookupNormalValue': self.lookupNormalValue,
            'lookupBillingCode': self.lookupBillingCode,
        }
        self.wit_client = Wit(access_token, actions)
        self.context = {}

    def say(self, session_id, context, msg):
        logger.debug('say:: session_id: {}, msg: {}'.format(session_id, msg))
        channel = session_id.split(':')[0]
        self.msg_writer.send_message(channel, msg)

    def merge(self, session_id, context, entities, msg):
        logger.debug('merge:: session_id: {}, context: {}, entities:{}, msg:{}'.format(session_id, context, entities, msg))
        hcv_val = first_entity_value(entities, 'heart_chamber_value')
        if hcv_val:
            context['hcv'] = hcv_val.lower()
        gender = first_entity_value(entities, 'gender')
        if gender:
            context['gender'] = gender.lower()
        age = first_entity_value(entities, 'patient_age')
        if age:
            age_nums = re.findall('\d+', age)
            if len(age_nums) > 0:
                context['age'] = age_nums[0]
        ventricle = first_entity_value(entities, 'ventricle_type')
        if ventricle:
            context['ventricle'] = ventricle.lower()
        bct = first_entity_value(entities, 'billing_code_type')
        if bct:
            context['bct'] = bct
        im = first_entity_value(entities, 'image_modality')
        if im:
            context['im'] = im
        anl = first_entity_value(entities, 'anatomy_loc')
        if anl:
            context['anl'] = anl
        # check has to come first to reverse priority
        if 'ct' in context:
            conf_stress = first_entity_value(entities, 'confirmation')
            if conf_stress:
                context['stress'] = conf_stress.lower()
        if 'anl' in context:
            conf_ct = first_entity_value(entities, 'confirmation')
            if conf_ct:
                context['ct'] = conf_ct.lower()


        logger.debug('end merge:: context: {}'.format(context))
        return context

    def lookupBillingCode(self, session_id, context):
        logger.debug('lookupBillingCode:: session_id: {}, context: {}'.format(session_id, context))

        # just for demo, in reality need catalog of billing codes given context values
        context['billing_code'] = '\n> *75559* - _Cardiac magnetic resonance imaging for morphology and function without contrast material; with stress imaging_'

        return context

    def lookupNormalValue(self, session_id, context):
        logger.debug('lookupNormalValue:: session_id: {}, context: {}'.format(session_id, context))
        if 'hcv' in context and 'gender' in context and 'age' in context and 'ventricle' in context:

            if context['age'] < 35:
                age_key = 'lt35'
            else:
                age_key = 'gte35'

            spec_norm_vals = abs_norm_values[context['gender']][age_key][context['ventricle']][context['hcv']]
            context['norm_val'] = spec_norm_vals['value']
            context['std_val'] = spec_norm_vals['sd']
            context['range_lower'] = spec_norm_vals['range_lower']
            context['range_upper'] = spec_norm_vals['range_upper']
            context['hcv_units'] = abs_norm_values['units'][context['hcv']]

        return context

    def lookupHCV(self, session_id, context):
        logger.debug('lookupHCV:: session_id: {}, context: {}'.format(session_id, context))
        if 'hcv' in context:
            hcv_val = context['hcv'].lower()
            if hcv_val == 'edv':
                context['hcv_meaning'] = '_end-diastolic volume_'
            elif hcv_val == 'esv':
                context['hcv_meaning'] = '_end-systolic volume_'
            elif hcv_val == 'ef':
                context['hcv_meaning'] = '_ejection fraction_'
            elif hcv_val == 'sv':
                context['hcv_meaning'] = '_stroke volume_'
            elif hcv_val == 'mass':
                context['hcv_meaning'] = '_myocardial mass_'
            else:
                context['hcv_meaning'] = 'unknown'

        return context

    def clearContext(self, session_id, context):
        logger.debug('clearContext:: session_id: {}, context: {}'.format(session_id, context))
        self.context = {}
        return self.context

    def error(self, session_id, context, e):
        logger.debug('error:: session_id: {}, context: {}, e: {}'.format(session_id, context, e))
        logging.error(str(e))

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
        if 'user' in event and not self.clients.is_message_from_me(event['user']):

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
                    self.clients.send_user_typing_pause(event['channel'], sleep_time=0.0)
                    logger.debug('Sending message: {} to wit_client actions for session_id: {} with self.context: {}'.format(msg_txt, session_id, self.context))
                    try:
                        self.context = self.wit_client.run_actions(session_id, msg_txt, self.context)
                    except:
                        err_msg = traceback.format_exc()
                        logging.error('Unexpected error: {}'.format(err_msg))
                        self.context = {}
                        self.msg_writer.send_message(event['channel'], "_Please see my logs for an error I encountered._")

    def _is_direct_message(self, channel_id):
        return channel_id.startswith('D')
