import json
import logging
import random
import re
import time
import traceback
import apiai

# TODO: make this configurable
access_token = '6a6d19088a2d49c2b0912c81c3661653'


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


class RtmEventHandler(object):
    def __init__(self, slack_clients, msg_writer):
        self.clients = slack_clients
        self.msg_writer = msg_writer
        self.action_matcher = re.compile('\[.*?\]')
        self.apiai_client = apiai.ApiAI(access_token)
        self.dbg_ctx = False
        self.context = {}

    def lookupBillingCode(self, anatomical_locale, image_modality, contrast_use, stress_use):
        logger.debug('lookupBillingCode:: anatomical_locale:{}, image_modality:{}, contrast_use:{}, stress_use:{}'.format(anatomical_locale, image_modality, contrast_use, stress_use))

        if contrast_use == 'yes':
            contrast_txt = 'with'
        else:
            contrast_txt = 'without'

        if stress_use == 'yes':
            stress_txt = 'with'
        else:
            stress_txt = 'without'

        # just for demo, in reality need catalog of billing codes given context values
        return '\n> *75559* - _Cardiac magnetic resonance imaging for morphology and function {} contrast material; {} stress imaging_'.format(contrast_txt, stress_txt)


    def lookupNormalValue(self, ventricle, hcv, gender, age_in_years):
        logger.debug('lookupNormalValue:: ventricle:{}, hcv:{}, gender:{}, aged_in_years:{}'.format(ventricle, hcv, gender, age_in_years))

        ventricle = ventricle.lower()
        hcv = hcv.lower()
        gender = gender.lower()
        age = age_in_years['age']

        if age < 35:
            age_key = 'lt35'
            age_phrase = 'younger than 35 years old'
        else:
            age_key = 'gte35'
            age_phrase = 'older than 35 years old'

        spec_norm_vals = abs_norm_values[gender][age_key][ventricle][hcv]

        return "The normal absolute {} {} for a {} {} is {} ({}) +/- {} or within the range from {} to {}".format(
            ventricle,
            hcv,
            gender,
            age_phrase,
            spec_norm_vals['value'],
            abs_norm_values['units'][hcv],
            spec_norm_vals['sd'],
            spec_norm_vals['range_lower'],
            spec_norm_vals['range_upper'],
        )

    def lookupAcronym(self, acronym):
        logger.debug('lookupAcronym:: acronym: {}'.format(acronym))
        acronym_lower = acronym.lower()
        if acronym_lower == 'edv':
            return '_end-diastolic volume_'
        elif acronym_lower == 'esv':
            return '_end-systolic volume_'
        elif acronym_lower == 'ef':
            return '_ejection fraction_'
        elif acronym_lower == 'sv':
            return '_stroke volume_'
        elif acronym_lower == 'mass':
            return '_myocardial mass_'
        else:
            return 'unknown'

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
                elif ';debug' in msg_txt:
                    if 'on' in msg_txt:
                        self.dbg_ctx = True
                    elif 'off' in msg_txt:
                        self.dbg_ctx = False
                    else:
                        self.msg_writer.send_message(event['channel'], '```context: {}```'.format(self.context))
                elif ';reset' in msg_txt:
                    logger.debug('Resetting context to {}')
                    self.context = {}
                else:
                    if msg_txt.startswith('<@U'):
                        # message starts with something like: `<@U11T7N1U3>: `... (so strip off user mention)
                        msg_txt = msg_txt[14:len(msg_txt)]
                    session_id = event['channel'] + ":" + event['user']
                    self.clients.send_user_typing_pause(event['channel'], sleep_time=0.0)
                    logger.debug('Sending message: {} to apiai_client for session_id: {} with self.context: {}'.format(msg_txt, session_id, self.context))
                    try:
                        start = time.time()
                        req = self.apiai_client.text_request()
                        req.query = msg_txt
                        response = req.getresponse()
                        resp_txt = response.read()
                        resp = json.loads(resp_txt)
                        end = time.time()
                        self._handle_apiai_response(resp, event)
                        if self.dbg_ctx:
                            self.msg_writer.send_message(event['channel'], '```{}```\n> _Took {} secs_'.format(resp_txt, end-start))
                    except:
                        err_msg = traceback.format_exc()
                        logging.error('Unexpected error: {}'.format(err_msg))
                        self.context = {}
                        self.msg_writer.send_message(event['channel'], "_Please see my logs for an error I encountered._")

    def _is_direct_message(self, channel_id):
        return channel_id.startswith('D')

    def _handle_apiai_response(self, resp, event):
        try:

            if resp['result']['score'] > 0.5:
                msg_resp = resp['result']['fulfillment']['speech']
                logger.debug('msg_resp: {}'.format(msg_resp))

                # searches the msg_resp for instances of '[action_name]', see if there is a function
                # of that same name and if we can extract the necessary parameters, and if so then
                # calls the function and replaces that string with the value returned from the function
                actions = self.action_matcher.findall(msg_resp)
                for action in actions:
                    action = action[1:len(action)-1]
                    #logger.debug('hasattr({}):{}'.format(action, hasattr(self, action)))
                    if hasattr(self, action):
                        act_func = getattr(self, action)
                        act_params = act_func.__code__.co_varnames[1:act_func.__code__.co_argcount]
                        act_param_vals = {}
                        for act_param in act_params:
                            if act_param in resp['result']['parameters'].keys():
                                act_param_vals[act_param] = resp['result']['parameters'][act_param]
                        logger.debug('act_param_vals: {}'.format(act_param_vals))
                        act_val = act_func(**act_param_vals)
                        msg_resp = msg_resp.replace('[{}]'.format(action), act_val)
                    else:
                        logger.error('Undefined action: {} parsed in response message: {}'.format(action, msg_resp))
                # end for action
                self.msg_writer.send_message(event['channel'], msg_resp)
            else:
                misunderstandings = [
                    "I'm sorry, I don't quite understand you... can you try saying it a different way?",
                    "I'm confused. Could you say it differently?",
                    "Unfortunately I'm not sure how to respond... I'm still learning and getting smarter every day.",
                    "Do you mind trying something else?  I didn't understand what you wanted.",
                    "Hmm... I'm not sure what you meant.  Can you elaborate?"
                ]
                self.msg_writer.send_message(event['channel'], random.choice(misunderstandings))
        except Exception as e:
            logger.exception('Failed to parse response from api.ai: {}', resp)
            self.msg_writer.send_message(event['channel'], "_Please see my logs for an error I encountered._")
