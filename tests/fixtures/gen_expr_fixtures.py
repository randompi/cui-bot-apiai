#!/usr/bin/env python

import logging
import os
import json
import time
import apiai

logger = logging.getLogger(__name__)
access_token = '6a6d19088a2d49c2b0912c81c3661653'

def parse_dir_of_jsons(path_to_jsons_dir):
    '''
    :param path_to_jsons_dir: Path the directory containing .json files
    :return: A dict of json file name to its loaded json dict
    '''
    json_map = {}
    json_files = [pos_json for pos_json in os.listdir(path_to_jsons_dir) if pos_json.endswith('.json')]
    for js in json_files:
        with open(os.path.join(path_to_jsons_dir, js)) as json_file:
            json_map[js.split('.')[0]] = json.load(json_file)
    return json_map


def write_json_to_file(json_dict, to_file_path):
    with open(to_file_path, 'w') as outfile:
        json.dump(json_dict, outfile, indent=4)


def make_apiai_request(apiai_cli, user_txt):
    time.sleep(0.25) # so we don't DoS apiai
    try:
        req = apiai_cli.text_request()
        req.query = user_txt
        response = req.getresponse()
        resp_txt = response.read()
        resp = json.loads(resp_txt)
        return resp
    except Exception as e:
        logging.error('Unexpected error: {}'.format(e))
        return ''

if __name__ == "__main__":
    #print 'Hello from gen_expr_fixtures!'
    apiai_exports_dir = '../../resources/api-ai-bak/Hermes-Demo'
    entities_json = parse_dir_of_jsons(os.path.join(apiai_exports_dir, 'entities'))
    intents_json = parse_dir_of_jsons(os.path.join(apiai_exports_dir, 'intents'))

    #print 'len(i_j): {}'.format(len(intents_json))
    #print 'keys: {}'.format(intents_json.keys())

    apiai_cli = apiai.ApiAI(access_token)
    intent_to_exprs = {}
    for intent_name, intent in intents_json.iteritems():
        print 'Processing {}...'.format(intent_name)

        intent_exprs = []
        for user_say in intent.get('userSays'):
            user_txt = ''

            for data_obj in user_say.get('data'):
                if 'meta' in data_obj:
                    # print '\tmeta: {}'.format(data_obj.get('meta'))
                    # TODO: we could generate variations of the expression by substituing entity synonyms here
                    pass
                user_txt += data_obj['text']

            resp = make_apiai_request(apiai_cli, user_txt)
            intent_exprs.append({user_txt:resp})

        intent_to_exprs[intent_name] = intent_exprs

    #print 'intent_to_exprs: {}'.format(intent_to_exprs)


    intent_exprs_file_path = './intent_to_exprs.json'
    write_json_to_file(intent_to_exprs, intent_exprs_file_path)

    print 'Wrote out {}'.format(intent_exprs_file_path)