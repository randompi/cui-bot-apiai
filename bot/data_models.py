# -*- coding: utf-8 -*-

import logging
import pandas as pd
import Levenshtein as ls
from collections import Counter

logger = logging.getLogger(__name__)

class DataModels(object):

    def __init__(self):
        self.data_frames = {}
        self.data_frames['Procedure_Revenue'] = pd.read_csv('resources/MedData_SQL_DB/Procedure_Revenue.csv')
        self.data_frames['Study_List'] = pd.read_csv('resources/MedData_SQL_DB/Study_List.csv')

    def selectData(self, parameters):
        dfs = []
        df_cols = []
        df_filters = []
        most_cmn_frame = ''

        # map incoming params to instances and entities in the data frames
        for param_k, param_v in parameters.iteritems():

            if param_k.startswith('row') and param_v != '':
                best_match_df = self.map_to_frame(param_v)
                dfs.append(best_match_df)

            if param_k.startswith('col') and param_v != '':
                best_match_col = self.map_to_frame_col(param_v)
                df_cols.append(best_match_col)

            # TODO: need to pair filter with column
            if param_k.startswith('filter') and param_v != '':
                df_filters.append(param_v)

        logger.debug('dfs: {}\ndf_cols: {}\ndf_filters: {}\nmost_cmn_frame: {}'.format(dfs, df_cols, df_filters, most_cmn_frame))

        results = []
        for df in dfs:
            frame = self.data_frames[df]
            if len(df_cols) > 0 and len(df_filters) > 0:
                conditions = True
                for col, df_filt in zip(df_cols, df_filters):
                    conditions = conditions & (frame[col]==df_filt)
                    results.append(frame[conditions])
            elif len(df_cols) > 0:
                results.append(frame[df_cols])
            else:
                results.append(frame.head())

        return results


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


    def map_to_frame(self, nl_frame_str):
        """Tries to find a corresponding frame that is most similar to the provided nl_frame_str

        Best match is found using Levenshtein distance comparisons

        Args:
            nl_frame_str (str): A natural language string that we want to compare similarity to frame names

        Returns:
            str: The DataFrame name that best matches the provided nl_frame_str
        """
        max_ls_ratio_tup = self.max_levenshtein_ratio(nl_frame_str, self.data_frames.keys())

        # TODO: may want to introduce minimum ratio threshold, and raise exception if too low

        return max_ls_ratio_tup[0]


    def map_to_frame_col(self, nl_col_str):
        """Tries to find a corresponding column in a frame that is most similar to the provided nl_col_str

        Best match is found using Levenshtein distance comparisons

        Args:
            nl_col_str (str): A natural language string that we want to compare similarity to column names

        Returns:
            string: The DataFrame column name that best matches the provided nl_col_str
        """
        all_col_names = []
        for df_name, df in self.data_frames.iteritems():
            all_col_names.extend(df.columns.values)
        max_ls_ratio_tup = self.max_levenshtein_ratio(nl_col_str, all_col_names)

        #TODO: may want to introduce minimum ratio threshold, and raise exception if too low

        return max_ls_ratio_tup[0]


    def max_levenshtein_ratio(self, target_str, comp_strs):
        """ Iterates through comp_strs comparing to target_str using Levenshtein ratio
            as a measure of how similar two strings are (higher is more similar).

            e.g. target_str = 'cat', comp_strs = ['card', 'bat', 'ford', 'rat'] -> ('bat', 0.66666)

            Args:
                target_str (str): The string we compare to all others in `comp_strs`.
                comp_strs (:obj:`list` of :obj:`str`): List of strings to compare target_str with.

            Returns:
                (:obj:`tuple` of :obj:`str` and :obj:`float`): A tuple of the first comp_str in `comp_strs`
                with the highest Levenshtein ratio to `target_str`.

                If `comp_strs` is empty, returns (None, 0.0)
        """
        max_ls_ratio = 0.0
        max_comp_str = None

        for comp_str in comp_strs:

            logger.debug('target_str: {} <-> comp_str: {}'.format(target_str, comp_str))
            # Levenshtein distance score, higher means less edits required to match
            ls_ratio = ls.ratio(target_str, comp_str)
            logger.debug('ls_ratio: {}\n'.format(ls_ratio))

            if ls_ratio > max_ls_ratio:
                max_ls_ratio = ls_ratio
                max_comp_str = comp_str

        logger.debug('max_comp_str: {}, max_ls_ratio: {}')

        return (max_comp_str, max_ls_ratio)


    def averageForGroup(self, df_name, group_name, col_to_avg):
        return self.data_frames[df_name].groupby(group_name).agg({col_to_avg:{'Average':'mean'}})

