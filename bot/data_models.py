# -*- coding: utf-8 -*-

import logging
import pandas as pd
import Levenshtein as ls

logger = logging.getLogger(__name__)

class DataModels(object):

    def __init__(self):
        self.data_frames = {}
        self.data_frames['Billing_Codes'] = pd.read_csv('resources/MedData_SQL_DB/Billing_Codes.csv')
        self.data_frames['Patient_Data'] = pd.read_csv('resources/MedData_SQL_DB/Patient_Data.csv')
        self.data_frames['Physician_List'] = pd.read_csv('resources/MedData_SQL_DB/Physician_List.csv')
        self.data_frames['Procedure_Revenue'] = pd.read_csv('resources/MedData_SQL_DB/Procedure_Revenue.csv')
        self.data_frames['Study_List'] = pd.read_csv('resources/MedData_SQL_DB/Study_List.csv')


    def queryData(self, parameters):
        groupables = []
        aggregables = []
        frame_to_cols = {}
        frame = None
        ops = []

        relevantFrames = self.findRelevantFrames(parameters)

        for param_k, param_v in parameters.iteritems():
            if param_k.startswith('group') and param_v != '':
                if param_v in relevantFrames:
                    groupables.append(relevantFrames[param_v])
                else:
                    raise Exception('Failed to map {} to relevant data frame'.format(param_v))

            if param_k.startswith('aggregable'):
                if param_v in relevantFrames:
                    aggregables.append(relevantFrames[param_v])
                    frame = relevantFrames[param_v][0][0]

            if param_k.startswith('op') and param_v != '':
                ops.append(param_v)

        logger.debug('groupables: {}\naggregables: {}\nframe: {}\nops: {}'.format(groupables, aggregables, frame, ops))

        df = self.data_frames[frame]
        for groupable in groupables:
            # Detect which frames need to be merged and on which keys
            if groupable[0][0] != frame:
                #TODO impl
                logger.debug('groupable:{} != frame:{}'.format(groupable[0][0], frame))
                raise Exception('Merge across tables not implemented yet')
            else:
                # groupable part of same frame as aggregable
                df = df.groupby(groupable[0][1])
                logger.debug('df.first() after groupbys:\n{}'.format(df.first()))

        if isinstance(df, pd.DataFrame):
            # there were no groupables so just select aggregables
            cols = []
            for aggregable in aggregables:
                cols.append(aggregable[0][1])

            result = df[cols]

            # TODO: try / except here if we are passing an op that DataFrame doesn't support
            if len(ops) > 0:
                op_func = getattr(pd.DataFrame, ops[0])
                result = ops[0] + ':\n' + str(op_func(result)) #apply operation on selected cols

        else:
            agg_map = {}
            #TODO: We need to handle a map of op -> aggregable
            for aggregable in aggregables:
                agg_map[aggregable[0][1]] = ops

            logger.debug('agg_map: {}'.format(agg_map))

            result = df.agg(agg_map)

        return result


    def findRelevantFrames(self, query_params):
        # TODO: Possibly use gensim for semantic cosine similarity?
        # http://radimrehurek.com/gensim/tut3.html
        # https://en.wikipedia.org/wiki/Cosine_similarity#Soft_cosine_measure

        # currently using Levenshtein distance:
        # https://en.wikipedia.org/wiki/Levenshtein_distance
        params_to_df_col_map = {}
        for q_param_k, q_param_v in query_params.iteritems():

            # only considering 'groupable' or 'aggregable' parameters with non-blank values
            if (q_param_k.startswith('group') or q_param_k.startswith('agg'))\
                    and q_param_v != '':

                # scan each frame's columns and find max similarity score
                max_score = 0.0
                for df_name, df in self.data_frames.iteritems():
                    for col_name in df.columns.values:
                        logger.debug('q_param_v: {} <-> col_name: {}'.format(q_param_v, col_name))
                        sim_score = ls.ratio(q_param_v, col_name)
                        logger.debug('score: {}\n'.format(sim_score))
                        if sim_score > max_score:
                            max_score = sim_score
                            params_to_df_col_map[q_param_v] = [(df_name, col_name, max_score)]
                        elif sim_score == max_score:
                            # we have a tie (probably same column name)
                            if q_param_v in params_to_df_col_map:
                                params_to_df_col_map[q_param_v].append((df_name, col_name, max_score))
                            else:
                                params_to_df_col_map[q_param_v] = [(df_name, col_name, max_score)]

        logger.debug('params_to_df_col_map: {}'.format(params_to_df_col_map))
        return params_to_df_col_map


    def averageForGroup(self, df_name, group_name, col_to_avg):
        return self.data_frames[df_name].groupby(group_name).agg({col_to_avg:{'Average':'mean'}})

