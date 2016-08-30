# -*- coding: utf-8 -*-

import collections
import json
import logging
import urllib
import re
import time
import traceback
import apiai
import memory
import os
import pandas as pd
import matplotlib.pyplot as plt

from apiai_client import ApiaiDevClient
from data_models import DataModels
import sqlyzer

# TODO: make this configurable
access_token = '6a6d19088a2d49c2b0912c81c3661653'
dev_access_token = '5b9a5f58880e46cba523fbebf4c1bce2'

pd.set_option('display.width', 120)
pd.set_option('display.max_colwidth', 12)

# TODO: Read these in from editable files
acronyms = {
    'edv': 'end-diastolic volume',
    'esv': 'end-systolic volume',
    'ef': 'ejection fraction',
    'sv': 'stroke volume',
    'mass': 'myocardial mass',
    'lv': 'left ventricle',
    'rv': 'right ventricle',
    'vf': 'ventricular fibrillation',
    'ra': 'right atrium',
    'la': 'left atria',
    'ege': 'early gadolinium enhancement',
    'mvo': 'microvascular obstruction',
    'pa': 'pulmonary artery',
    'lad': 'left anterior descending artery',
    'cx': 'circumflex artery',
    'gd': 'gadolinium',
    'lvot': 'left ventricular outflow tract',
}
abs_norm_values = {
    'male':{
        'lt35':{
            'lv':{
                'edv':{'value': 173, 'sd': 29, 'range_lower': 115, 'range_upper': 231},
                'esv':{'value': 57, 'sd': 15, 'range_lower': 27, 'range_upper': 87},
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

protocols = {
    'myocardial_disease' : {
        'DCM' : [0,1,2,3,7],
        'HCM' : [0,1,4,5,7],
        'LVNC' : [0,1,7],
        'ARVC' : [0,1,3,7],
        'Amyloidosis' : [0,1,2,6,7,8],
        'Sarcoidosis' : [0,1,3,7],
        'Endomyocardial Fibrosis' : [0,1,2,6,7],
        'Iron Overload' : [0,1,2,9],
        'Tako-Tsubo Cardiomyopathy' : [0,1,2,3,7],
        'Myocarditis' : [0,1,2,3,7]
    },
}

protocol_modules = [
    'Anatomy module',
    'LV Function module',
    'Edema module',
    'RV Function module',
    'Velocity Encoding',
    'LV tagging',
    'EGE',
    'LGE module',
    'T1 Mapping',
    'T2* Mapping',
]

handle_data_actions = [
    'handleDataQuery',
    'handleSelectData',
    'handlePlotData',
]

col1 = 'BillingCodeID'
col2 = 'BillingCodeDesc'
col3 = 'ImageModality'
col4 = 'ConstrastMaterial'
col5 = 'StressImaging'
bc_df = pd.DataFrame([
    {col1:75557,
     col2:'Cardiac magnetic resonance imaging for morphology and function without contrast material',
     col3: 'MRI', col4: 'Without', col5: False},
    {col1:75559,
     col2:'Cardiac magnetic resonance imaging for morphology and function without contrast material; with stress imaging',
     col3: 'MRI', col4: 'Without', col5: True},
    {col1: 75561,
     col2: 'Cardiac magnetic resonance imaging for morphology and function without contrast material(s), followed by contrast material(s) and further sequences',
     col3: 'MRI', col4: 'With', col5: False},
    {col1: 75563,
     col2: 'Cardiac magnetic resonance imaging for morphology and function without contrast material(s), followed by contrast material(s) and further sequences; with stress imaging',
     col3: 'MRI', col4: 'With', col5: True},
    {col1: 75574,
     col2: 'Computed tomographic angiography, heart, coronary arteries and bypass grafts (when present), with contrast material, including 3D image postprocessing (including evaluation of cardiac structure and morphology, assessment of cardiac function, and evaluation of venous structures, if performed)',
     col3: 'CT', col4: 'With', col5: False},
    {col1: 75571,
     col2: 'Computed tomography, heart, without contrast material, with quantitative evaluation of coronary calcium',
     col3: 'CT', col4: 'Without', col5: False},
])


logger = logging.getLogger(__name__)

class BotUnknownException(Exception):
    pass

class RtmEventHandler(object):

    def __init__(self, slack_clients, msg_writer):
        self.clients = slack_clients
        self.msg_writer = msg_writer
        self.dm = DataModels()
        self.action_matcher = re.compile('\[.*?\]')
        self.apiai_client = apiai.ApiAI(access_token)
        self.apiai_dev_client = ApiaiDevClient(dev_access_token)
        self.persist_client = memory.InMemPersister()
        self.sqlyzer = None
        self.dbg_ctx = False
        self.context = {}
        self.intent_to_teach = None
        self.user_is_teaching = False

        persist_token = os.getenv('BEEPBOOP_TOKEN')
        persist_url = os.getenv('BEEPBOOP_PERSIST_URL')
        if persist_token is not None and persist_url is not None:
            logger.info('Persistence env present - persist_token: {}, persist_url: {}'.format(persist_token, persist_url))
            self.persist_client = memory.BeepBoopPersister(persist_url, persist_token)
            # fetch latest from persistence on start up
            self.cardio_acronyms = self.persist_client.get('acronyms')
        else:
            self.cardio_acronyms = acronyms


    def lookupBillingCode(self, anatomical_locale, image_modality, contrast_use, stress_use, sql_select):
        logger.debug('lookupBillingCode:: anatomical_locale:{}, image_modality:{}, contrast_use:{}, stress_use:{}'.format(anatomical_locale, image_modality, contrast_use, stress_use))

        if contrast_use == 'yes':
            contrast_txt = 'With'
        else:
            contrast_txt = 'Without'

        if stress_use == 'True':
            stress_bool = True
            stress_txt = 'With'
        else:
            stress_bool = False
            stress_txt = 'without'

        if sql_select is not None and len(sql_select) > 0 and self.sqlyzer is not None:
            #TODO: Think about how to generalize this
            params = {'image_modality':image_modality, 'contrast_use':contrast_txt, 'stress_use':stress_bool}
            select_stmt = self.sqlyzer.generate_select_stmt(sql_select, params)
        else:
            select_stmt = ''

        results = bc_df[(bc_df[col3]==image_modality) & (bc_df[col4]==contrast_txt) & (bc_df[col5]==stress_bool)].to_string()

        return '{}\n*Result(s)*:```{}```'.format(select_stmt, results)


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
        if acronym_lower in self.cardio_acronyms:
            return '_{}_'.format(self.cardio_acronyms[acronym_lower])
        else:
            raise BotUnknownException('Unknown acronym: {}'.format(acronym_lower))


    def lookupProtocol(self, myocardial_disease):
        logger.debug('lookupProtocol:: myocardial_disease: {}'.format(myocardial_disease))
        protocol_str = 'The SCMR recommended protocol for _{}_ is:\n'.format(myocardial_disease)
        proto_mods = protocols['myocardial_disease'][myocardial_disease]
        for proto_mod in proto_mods:
            protocol_str += '> {}\n'.format(protocol_modules[proto_mod])
        return protocol_str


    def handleDataQuery(self, parameters):
        logger.debug('handleDataQuery:: parameters: {}'.format(parameters))

        try:
            df = self.dm.queryData(parameters)
            result = '_*Results:*_\n```{}```'.format(df)
        except Exception as e:
            result = ':x: {}'.format(e.message)

        return result


    def handleSelectData(self, parameters):
        logger.debug('handleSelectData:: parameters: {}'.format(parameters))

        try:
            data_list = self.dm.selectData(parameters)
            result = '_*Results:*_\n'
            for i, data in enumerate(data_list):
                if i < len(data_list) - 1:
                    if self.dbg_ctx:
                        result += data + '\n'
                else:
                    result += '```{}```'.format(data)
        except Exception as e:
            result = ':x: {}'.format(e.message)

        return result


    def handlePlotData(self, parameters):
        logger.debug('handleSelectData:: parameters: {}'.format(parameters))

        self.clients.send_user_typing_pause(parameters['channel'], sleep_time=0.0)

        plot = self.dm.plotData(parameters)
        if plot:
            fig = plot.get_figure()
            plot_name = 'plot.png'
            fig.savefig(plot_name)
            self.msg_writer.upload_file(plot_name, parameters['channel'])
            return None
        else:
            if self.dm.prev_ret_data is None:
                return ':x: _{}_'.format('Need to query for data before we can plot it, or `;reset` context.')
            else:
                return ':x: _{}_\n```{}```'.format('Failed to map parameters to data entities.', parameters)


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
            if msg_txt.startswith('<@U'):
                # message starts with something like: `<@U11T7N1U3>: `... (so strip off user mention)
                msg_txt = msg_txt[14:len(msg_txt)]

            if self.clients.is_bot_mention(msg_txt) or self._is_direct_message(event['channel']):


                if 'help' in msg_txt:
                    self.msg_writer.write_help_message(event['channel'])

                elif ';reset' in msg_txt:
                    logger.debug('Resetting context to {}')
                    self.context = {}
                    self.dm.prev_ret_data = None

                elif ';debug' in msg_txt:
                    if 'on' in msg_txt:
                        self.dbg_ctx = True
                    elif 'off' in msg_txt:
                        self.dbg_ctx = False
                    else:
                        self.msg_writer.send_message(event['channel'], '```context: {}```'.format(self.context))

                elif ';stats' in msg_txt:
                    self._handle_stats(msg_txt, event)

                elif ';frames' in msg_txt:
                    self._handle_frames(msg_txt, event)

                elif ';sqltables' in msg_txt:
                    self.msg_writer.send_message(event['channel'], '```sqltables: {}```'.format(self.sqlyzer.tables))

                # things like: ;list, ;get, ;set, and ;del
                elif self._handle_persist_commands(msg_txt, event):
                    pass

                elif self._handle_parse_sql_schema(msg_txt, event):
                    pass

                elif self.context.get('is_mapping_schema'):
                    self._handle_schema_mapping(msg_txt, event)

                elif msg_txt.startswith(';learn'):
                    cmd_parts = msg_txt.split(' ')
                    if len(cmd_parts) >= 3:
                        if cmd_parts[1] == 'acronym':
                            if '=' in cmd_parts[2]:
                                acronym_eq = ' '.join(cmd_parts[2:])
                                self._learn_acronym(acronym_eq)
                                self.msg_writer.send_message(event['channel'], 'Ok, from now on I will remember that `{}`. Thanks! :mortar_board:'.format(acronym_eq))
                                return
                            else:
                                self.msg_writer.send_message(event['channel'], 'There was not an `=` in {}'.format(cmd_parts[2]))
                        else:
                            self.msg_writer.send_message(event['channel'], 'At the moment I only know how to learn an `acronym` and I do not know what a `{}` is... :disappointed:'.format(cmd_parts[1]))
                    else:
                        self.msg_writer.send_message(event['channel'], 'I had a problem learning that, I was expecting 3 arguments like: `;learn acronym EF=ejection fraction`')
                elif self.user_is_teaching is True:
                    if msg_txt.lower().startswith('y'):
                        for param in self.intent_to_teach['result']['parameters'].keys():
                            self.msg_writer.send_message(event['channel'], 'What is the `{}` in `{}`?\n(e.g. you can say something like: `LV=left ventricle`)'.format(param, self.intent_to_teach['result']['resolvedQuery']))
                    elif '=' in msg_txt:
                        self._learn_acronym(msg_txt)
                        self.msg_writer.send_message(event['channel'], 'Ok, from now on I will remember that `{}`. Thanks! :mortar_board:'.format(msg_txt))
                        self.msg_writer.send_message(event['channel'], 'You can also teach me using the `;learn` command, e.g. `;learn acronym LV=left ventricle`'.format(msg_txt))
                        self.user_is_teaching = False
                        self.intent_to_teach = None
                    else:
                        self.msg_writer.send_message(event['channel'], 'Sorry, you will have to train me in api.ai')
                        self.user_is_teaching = False
                        self.intent_to_teach = None
                elif self.intent_to_teach is not None:
                    if msg_txt.lower().startswith('y'):
                        self.msg_writer.send_message(event['channel'], 'Were you intending to: {}?'.format(self.intent_to_teach['result']['metadata']['intentName']))
                        self.user_is_teaching = True
                    else:
                        self.intent_to_teach = None
                        self.msg_writer.send_message(event['channel'], 'Ok, no worries! :simple_smile:')
                else:
                    self._request_to_apiai(msg_txt, event, self._handle_apiai_response)


    def _handle_stats(self, msg_txt, event):

        stats_parts = msg_txt.split(' ')
        if len(stats_parts) >= 4:
            data = self.dm.averageForGroup(stats_parts[1], stats_parts[2], stats_parts[3])
            self.msg_writer.send_message(event['channel'], '```{}```'.format(data))
        else:
            self.msg_writer.send_message(event['channel'], 'Error, expecting args like: `;stats <table_name> <groupby> <col>`')


    def _handle_frames(self, msg_txt, event):

        frame_parts = msg_txt.split(' ')

        if len(frame_parts) == 1:
            frame_summary = 'These are the frames I currently _know_ about:\n'
            for f_k, f_v in self.dm.data_frames.iteritems():
                frame_summary += '> *{}* _({} rows, {} cols)_\n'.format(f_k, f_v.shape[0], f_v.shape[1])
            frame_summary += 'Type `;frames <name>` to get more details.'
            self.msg_writer.send_message(event['channel'], '{}'.format(frame_summary))

        elif len(frame_parts) == 2:
            frame_details = ''
            f_name = frame_parts[1]
            head_num = 3
            if f_name in self.dm.data_frames.keys():
                frame_details += '*{}* _(first {} rows)_:\n```{}```'.format(f_name, head_num, self.dm.data_frames[f_name].head(head_num))
            else:
                frame_details += '`\'{}\'` is not a valid frame, should be one of:\n>  `{}`'.format(f_name, self.dm.data_frames.keys())
            self.msg_writer.send_message(event['channel'], '{}'.format(frame_details))


    def _handle_persist_commands(self, msg_txt, event):

        if ';get' in msg_txt:
            get_parts = msg_txt.split(' ')
            if len(get_parts) >= 2:
                try:
                    val = self.persist_client.get(get_parts[1])
                    self.msg_writer.send_message(event['channel'], '```{}```'.format(val))
                except memory.PersistenceException as pe:
                    self.msg_writer.send_message(event['channel'], 'Sorry I encountered a problem:\n```{}```'.format(pe))
            return True

        elif ';set' in msg_txt:
            set_parts = msg_txt.split(' ')
            if len(set_parts) >= 3:
                try:
                    self.persist_client.set(set_parts[1], set_parts[2])
                    self.msg_writer.send_message(event['channel'], 'Saved: ```{} : {}```'.format(set_parts[1], set_parts[2]))
                except memory.PersistenceException as pe:
                    self.msg_writer.send_message(event['channel'], 'Sorry I encountered a problem:\n```{}```'.format(pe))
            elif len(set_parts) == 2:
                if set_parts[1] == 'acronyms':
                    try:
                        self.persist_client.set('acronyms', acronyms)
                        self.msg_writer.send_message(event['channel'], 'Saved: ```{} : {}```'.format('acronyms', acronyms))
                    except memory.PersistenceException as pe:
                        self.msg_writer.send_message(event['channel'], 'Sorry I encountered a problem:\n```{}```'.format(pe))
            return True

        elif ';list' in msg_txt:
            list_parts = msg_txt.split(' ')
            begins_with = None
            if len(list_parts) >= 2:
                begins_with = list_parts[1]
            try:
                keys = self.persist_client.list(begins_with=begins_with)
                self.msg_writer.send_message(event['channel'],
                                             'Keys: ```{}```'.format(keys))
            except memory.PersistenceException as pe:
                self.msg_writer.send_message(event['channel'],
                                             'Sorry I encountered a problem:\n```{}```'.format(pe))
            return True

        elif ';del' in msg_txt:
            del_parts = msg_txt.split(' ')
            if len(del_parts) >= 2:
                try:
                    self.persist_client.delete(del_parts[1])
                    self.msg_writer.send_message(event['channel'], 'Deleted: ```{}```'.format(del_parts[1]))
                except memory.PersistenceException as pe:
                    self.msg_writer.send_message(event['channel'], 'Sorry I encountered a problem:\n```{}```'.format(pe))
            return True

        return False


    def _handle_parse_sql_schema(self, msg_txt, event):

        if ';parse' in msg_txt:
            cmd_parts = msg_txt.split(' ')
            if len(cmd_parts) == 2:
                if cmd_parts[1] == 'BillingCodes.sql':
                    self.msg_writer.send_message(event['channel'], 'Parsing {}: ```{}```'.format(cmd_parts[1], sqlyzer.billing_code_schema[0]))
                    try:
                        self.sqlyzer = sqlyzer.Sqlyzer(sqlyzer.billing_code_schema[0])
                    except Exception as e:
                        logger.error('Failed parsing with exception: {}'.format(e))
                        return False

                    self.msg_writer.send_message(event['channel'], '> Parsed: ```{}```'.format(self.sqlyzer.tables))
                    self.context['is_mapping_schema'] = True
                    self.context['mapping_tables'] = [self.sqlyzer.tables.keys()[0]]
                    self._handle_schema_mapping(msg_txt, event)

                    return True


    def _handle_schema_mapping(self, msg_txt, event):

        # prep message string for comparison to column names
        msg_txt = msg_txt.encode('ascii', 'ignore')

        if self.context.get('confirm_intent'):
            self._request_to_apiai(msg_txt, event, self._handle_confirm_intent)
            return

        mapping_tables = self.context.get('mapping_tables')
        if mapping_tables:
            mapping_columns = self.context.get('mapping_columns')
            if mapping_columns is None:
                self._handle_table_mapping(mapping_tables, event)
            elif mapping_columns is True:
                table_to_map = self.sqlyzer.tables.get(mapping_tables[-1])
                # Expecting message format of <column_nameA>=<entity_paramB>, <column_nameB>=<entity_paramC>
                equalities = msg_txt.replace(' ', '').split(',')
                for equality in equalities:
                    eq_parts = equality.split('=')
                    if len(eq_parts) == 2:
                        mapped_eq = False
                        for col in table_to_map.get('columns'):
                            logger.debug('eq_parts:{}, col.name: {}'.format(eq_parts, col.get('name')))
                            if col.get('name') == eq_parts[0]:
                                col['entity'] = eq_parts[1]
                                mapped_eq = True
                                break
                            elif col.get('name') == eq_parts[1]:
                                col['entity'] = eq_parts[0]
                                mapped_eq = True
                                break
                        if not mapped_eq:
                            self.msg_writer.send_message(event['channel'], ':x: Failed to find corresponding column for: `{}`'.format(equality))
                    else:
                        self.msg_writer.send_message(event['channel'], ':x: Failed to map: `{}`'.format(equality))
                logger.debug('sqlyzer.tables: {}'.format(self.sqlyzer.tables))
                self.msg_writer.send_message(event['channel'],
                                             'Ok I\'ve mapped columns: `{}`'.format(table_to_map))

                # check for difference to see if there are more tables to map
                tables_to_map = set(self.sqlyzer.tables.keys()) - set(mapping_tables)
                if tables_to_map:
                    mapping_tables.append(tables_to_map.pop())
                    self.context.set('mapping_tables', mapping_tables)
                    del self.context['mapping_columns']
                    self._handle_table_mapping(mapping_tables, event)
                else:
                    self.msg_writer.send_message(event['channel'], 'Done mapping all tables in schema to the intent.  Feel free to use natural language to query your data. :speaking_head_in_silhouette:')
                    del self.context['mapping_columns']
                    del self.context['mapping_tables']
                    del self.context['is_mapping_schema']


    def _handle_table_mapping(self, mapping_tables, event):
        self.msg_writer.send_message(event['channel'],
                                     'Can you give an example of how you\'d naturally ask for data in `{}`?'.format(
                                         mapping_tables[-1]))
        self.context['confirm_intent'] = True

    def _handle_confirm_intent(self, resp, event):
        logger.debug('_handle_confirm_intent::')
        if resp['result']['score'] > 0.3:
            table_to_map = self.sqlyzer.tables.get(self.context.get('mapping_tables')[-1])
            table_to_map['intent_name'] = resp['result']['metadata']['intentName']
            self.msg_writer.send_message(event['channel'], 'Found matching intent: `{}`'.format(resp['result']['metadata']['intentName']))
            columns = table_to_map.get('columns')
            entities = resp['result']['parameters'].keys()
            self.msg_writer.send_message(event['channel'],
                                         'Please equate which columns in your table: `{}`\n map to these entities: `{}`\n (e.g. you can write: `{}={}, {}={}`)'.format(columns, entities, columns[0].get('name'), entities[0], columns[1].get('name'), entities[1]))
            self.context['mapping_columns'] = True
            del self.context['confirm_intent']
        else:
            self.msg_writer.send_message(event['channel'], 'I couldn\'t find a matching intent. :frowning:')
            del self.context['is_mapping_schema']


    def _learn_acronym(self, acronym_eq):
        txt_parts = acronym_eq.split('=')
        logger.info("msg_txt: {}, txt_parts: {}, cardio_acronyms: {}".format(acronym_eq, txt_parts, self.cardio_acronyms))
        self.cardio_acronyms[txt_parts[0].lower()] = txt_parts[1]
        try:
            self.persist_client.set('acronyms', self.cardio_acronyms)
        except memory.PersistenceException as pe:
            logger.error('Caught memory.PersistenceException: {}'.format(pe))
        entry = [
            {
                'value': txt_parts[0],
                'synonyms': [
                    txt_parts[0],
                    txt_parts[0].lower(),
                    txt_parts[1]
                ]
            }
        ]
        self.apiai_dev_client.post_entry('6cedca53-6027-44d2-8fd3-d6cf934ca920', entry)


    def _is_direct_message(self, channel_id):
        return channel_id.startswith('D')


    def _request_to_apiai(self, msg_txt, event, response_handler):
        session_id = event['channel'] + ":" + event['user']
        self.clients.send_user_typing_pause(event['channel'], sleep_time=0.0)
        logger.debug(
            'Sending message: {} to apiai_client for session_id: {} with self.context: {}'.format(msg_txt, session_id,
                                                                                                  self.context))
        try:
            start = time.time()
            req = self.apiai_client.text_request()
            req.query = msg_txt
            response = req.getresponse()
            resp_txt = response.read()
            resp = json.loads(resp_txt)
            end = time.time()
            response_handler(resp, event)
            if self.dbg_ctx:
                self.msg_writer.send_message(event['channel'],
                                             '```{}```\n> _Took {} secs_'.format(resp_txt, end - start))
        except:
            err_msg = traceback.format_exc()
            logging.error('Unexpected error: {}'.format(err_msg))
            self.context = {}
            self.msg_writer.send_message(event['channel'], "_Please see my logs for an error I encountered._")

    def _handle_apiai_response(self, resp, event):
        logger.debug('_handle_apiai_response::')
        try:

            confidence = 0.7  #TODO - make configurable
            if resp['result']['score'] > confidence:
                msg_resp = resp['result']['fulfillment']['speech']
                logger.debug('msg_resp: {}'.format(msg_resp))
                if not msg_resp:
                    msg_resp = '[{}]'.format(resp['result']['action'])

                # searches the msg_resp for instances of '[action_name]', see if there is a function
                # of that same name and if we can extract the necessary parameters, and if so then
                # calls the function and replaces that string with the value returned from the function
                actions = self.action_matcher.findall(msg_resp)
                for action in actions:
                    action = action[1:len(action)-1]
                    #logger.debug('hasattr({}):{}'.format(action, hasattr(self, action)))
                    if hasattr(self, action):
                        act_func = getattr(self, action)
                        if action in handle_data_actions:
                            params = self._cleanse(resp['result']['parameters'])
                            if action.startswith('handlePlotData'):
                                params['channel'] = event['channel']
                            msg_resp = act_func(params)
                        else:
                            act_params = act_func.__code__.co_varnames[1:act_func.__code__.co_argcount]
                            act_param_vals = {}
                            for act_param in act_params:
                                if act_param in resp['result']['parameters'].keys():
                                    act_param_vals[act_param] = resp['result']['parameters'][act_param]
                            logger.debug('act_param_vals: {}'.format(act_param_vals))
                            try:
                                act_val = act_func(**act_param_vals)
                                msg_resp = msg_resp.replace('[{}]'.format(action), act_val)
                            except BotUnknownException as botUnkE:
                                logger.warning('Caught BotUnknownException: {}'.format(botUnkE))
                                # TODO: See if user can teach the bot?

                                self.msg_writer.send_message(event['channel'], "Unfortunately I don't know that.  Would you like to teach me?")
                                self.intent_to_teach = resp
                                return
                    else:
                        logger.error('Undefined action: {} parsed in response message: {}'.format(action, msg_resp))
                # end for action
                if msg_resp:
                    self.msg_writer.send_message(event['channel'], msg_resp)
            else:
                if resp['result']['source'] == 'agent':
                    low_confidence_msg = "I'm sorry, I haven't been trained enough to understand that query yet."
                    confidence_pct = resp['result']['score'] * 100
                    detected_intent = resp['result'].get('metadata').get('intentName')
                    low_confidence_msg += "\n_I was only {:.0f}% confident that your intent was: {}_".format(confidence_pct, detected_intent)
                    self.msg_writer.send_message(event['channel'], low_confidence_msg)
                elif resp['result']['source'] == 'domains':
                    speech_resp = resp['result'].get('fulfillment').get('speech')
                    if speech_resp:
                        self.msg_writer.send_message(event['channel'], speech_resp)
                    else:
                        query = urllib.urlencode({'q': event['text']})
                        google_search_url = 'http://www.google.com/search?' + query
                        self.msg_writer.send_message(event['channel'], "I don't know, but you could try: {}".format(google_search_url))
                else:
                    self.msg_writer.send_message(event['channel'], "Sorry I don't know how to answer that yet.")
        except Exception as e:
            logger.exception('Failed to parse response from api.ai: {}', resp)
            self.msg_writer.send_message(event['channel'], "_Please see my logs for an error I encountered._")


    def _cleanse(self, data):
        if isinstance(data, basestring):
            return data.encode('utf-8')
        elif isinstance(data, collections.Mapping):
            return dict(map(self._cleanse, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(self._cleanse, data))
        else:
            return data
