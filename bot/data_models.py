# -*- coding: utf-8 -*-

import logging
import pandas as pd
import Levenshtein as ls
import re
from collections import namedtuple
from collections import Counter

logger = logging.getLogger(__name__)

compstr_to_pyopstr = {
    'less than' : '<',
    'greater than' : '>',
    'equals' : '==',
    'not equals' : '!='
}

class DataModels(object):

    def __init__(self):
        self.data_frames = {}
        self.data_frames['Procedure_Revenue'] = pd.read_csv('resources/MedData_SQL_DB/Procedure_Revenue.csv')
        self.data_frames['Study_List'] = pd.read_csv('resources/MedData_SQL_DB/Study_List.csv')
        self.prev_ret_data = None # last Pandas data object returned as result of query
        self.trailing_digit_re = re.compile('.*([0-9])$')
        self.ColQuery = namedtuple('ColQuery', ['col','filter','comp'])

    def selectData(self, parameters):
        most_cmn_frame = ''

        non_empty_params = {}

        # map incoming params to instances and entities in the data frames
        for param_k, param_v in parameters.iteritems():

            if param_k.startswith('row') and param_v != '':
                best_match_df = self.map_to_frame(param_v)
                most_cmn_frame = best_match_df

            if param_v:
                non_empty_params[param_k] = param_v

        col_queries = self.generate_column_queries(parameters)

        logger.debug('col_queries: {}'.format(col_queries))

        results = []
        results.append('> api.ai Parameters: {}'.format(non_empty_params))
        results.append('> column constraints: {}'.format(col_queries))
        frame = self.data_frames[most_cmn_frame]
        if col_queries and len(col_queries) > 0:
            # query data by columns
            constraints = True
            for col_query in col_queries:
                if col_query.filter:
                    # produces something like: frame['Age']>25
                    col_constraint = self._gen_col_constraint_str(col_query)
                    logger.debug('col_constraints: {}'.format(col_constraint))
                    constraints = constraints & (eval(col_constraint))
            # TODO: apply column selection qualifiers? (instead of currently returning all columns)
            self.prev_ret_data = frame[constraints]
            results.append(frame[constraints])
            # TODO: handle the case when there are no filter constraints
        else:
            # query all the data in the frame
            self.prev_ret_data = frame
            results.append('_First 5 rows of {}_'.format(len(frame)))
            results.append(frame.head(5))

        return results


    def _gen_col_constraint_str(self, col_query):
        if isinstance(col_query.filter, basestring):
            return 'frame[\'{}\']{}\'{}\''.format(col_query.col, col_query.comp, col_query.filter)
        else:
            return 'frame[\'{}\']{}{}'.format(col_query.col,col_query.comp,col_query.filter)

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

        self.prev_ret_data = result
        return result


    def plotData(self, params):
        """ Maps params to the data frame entities that are intended to be plot,
            and then determines the appropriate style for the plot, returns generated plot
        """

        cols = []
        x_col = None
        y_col = None
        plot_type = 'line' # default style

        # map incoming params to instances and entities in the data frames
        for param_k, param_v in params.iteritems():

            if param_v:
                if param_k.startswith('col'):
                    cols.append(self.map_to_frame_col(param_v))

                if param_k.startswith('x'):
                    x_col = self.map_to_frame_col(param_v)

                if param_k.startswith('y'):
                    y_col = self.map_to_frame_col(param_v)

                if param_k.startswith('plot-type'):
                    plot_type = param_v

        # check to see if we're plotting data that has been queried and returned before tracked
        # in prev_ret_data, or if we need to infer the appropriate frame from params
        if self.prev_ret_data is None:
            if 'row' in params and params['row']:
                frame_name = self.map_to_frame(params['row'])
                data_to_plot_from = self.data_frames[frame_name]
            elif cols:
                most_ref_frame_name = self.find_most_referred_frame(cols)
                if most_ref_frame_name:
                    data_to_plot_from = self.data_frames[most_ref_frame_name]
                else:
                    # couldn't reconcile which data objects we should be plotting
                    return None
            else:
                # couldn't reconcile which data objects we should be plotting
                return None
        else:
            data_to_plot_from = self.prev_ret_data

        # create a plot based on the variables and mappings we've been able to extract from params
        if x_col and y_col and x_col in data_to_plot_from.columns and y_col in data_to_plot_from.columns:
            # plot x vs y
            plot = data_to_plot_from.plot(x=x_col, y=y_col, kind=plot_type)
        elif x_col and x_col in data_to_plot_from.columns:
            # plot over a single column
            cols.append(x_col) # either add to existing cols, or if empty just add x_col
            plot = data_to_plot_from[cols].plot(x=x_col, kind=plot_type)
        elif cols:
            # plot a several columns
            # removing in place any columns that don't appear in the data_to_plot_from
            cols[:] = [col for col in cols if col in data_to_plot_from.columns]
            plot = data_to_plot_from[cols].plot(kind=plot_type)
        else:
            # plot all data
            plot = data_to_plot_from.plot(kind=plot_type)

        return plot


    def find_most_referred_frame(self, columns):
        """ Assuming columns is a list of column names in our data_frames
            which is the frame that has the most columns in it
        """
        f_name_counts = dict.fromkeys(self.data_frames.keys(), 0)
        for col in columns:
            for f_name, frame in self.data_frames.iteritems():
                if col in frame.columns:
                    f_name_counts[f_name] += 1

        return Counter(f_name_counts).most_common(1)[0][0]

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

            #logger.debug('target_str: {} <-> comp_str: {}'.format(target_str, comp_str))
            # Levenshtein distance score, higher means less edits required to match
            ls_ratio = ls.ratio(target_str, comp_str)
            #logger.debug('ls_ratio: {}\n'.format(ls_ratio))

            if ls_ratio > max_ls_ratio:
                max_ls_ratio = ls_ratio
                max_comp_str = comp_str

        #logger.debug('max_comp_str: {}, max_ls_ratio: {}')

        return (max_comp_str, max_ls_ratio)


    def generate_column_queries(self, params_map):
        """ Given a map of parameters containing keys like col1, filter1, and comparison-filter1
            generate a ColQuery namedtuple of these groupings (filter and comp could be None)
        """
        col_queries = []

        if params_map:
            for p_k, p_v in params_map.iteritems():

                filter_val = None
                comp_val = None
                if p_k.startswith('col') and p_v:
                    best_match_col = self.map_to_frame_col(p_v)

                    col_num = ''
                    col_num_matches = self.trailing_digit_re.findall(p_k)
                    if col_num_matches:
                        col_num = col_num_matches[0]

                    filter_key = 'filter' + col_num
                    number_key = 'number' + col_num
                    if filter_key in params_map and params_map[filter_key]:
                        filter_val = params_map[filter_key]
                    elif number_key in params_map and params_map[number_key]:
                        filter_val = params_map[number_key]

                    if  filter_val:
                        try:
                            filter_val = self.coerce_str_to_numeric(filter_val)
                        except:
                            # filter_val not a numeric so just leave as a string
                            pass
                        comparator_key = 'comparison-filter' + col_num
                        if comparator_key in params_map and params_map[comparator_key]:
                            comp_str = params_map[comparator_key]
                            if comp_str in compstr_to_pyopstr:
                                comp_val = compstr_to_pyopstr[comp_str]
                            else:
                                comp_val = '=='
                        else:
                            comp_val = '=='

                    col_queries.append(self.ColQuery(col=best_match_col, filter=filter_val, comp=comp_val))

                elif p_k.startswith('filter') and p_v:
                    filter_val = p_v
                    filt_num = ''
                    filt_num_matches = self.trailing_digit_re.findall(p_k)
                    if filt_num_matches:
                        filt_num = filt_num_matches[0]

                    col_key = 'col' + filt_num
                    if col_key in params_map and params_map[col_key]:
                        # we'll just defer to when the col gets processed above
                        continue
                    else:
                        col_has_val = self.find_col_with_val(p_v)
                        if col_has_val:
                            comparator_key = 'comparison-filter' + filt_num
                            if comparator_key in params_map and params_map[comparator_key]:
                                comp_str = params_map[comparator_key]
                                if comp_str in compstr_to_pyopstr:
                                    comp_val = compstr_to_pyopstr[comp_str]
                                else:
                                    comp_val = '=='
                            else:
                                comp_val = '=='

                        col_queries.append(self.ColQuery(col=col_has_val, filter=filter_val, comp=comp_val))


        return col_queries

    def find_col_with_val(self, val):

        for f_name, frame in self.data_frames.iteritems():
            for col in list(frame.select_dtypes(include=['object']).columns):
                if val.lower() in [col_val.lower() for col_val in frame[col].unique()]:
                    return col

        return None

    def coerce_str_to_numeric(self, x):

        try:
            f = float(x)
        except:
            raise ValueError("failed to coerce str to float")

        try:
            i = int(x)
        except:
            # not an int, but is a float
            return f

        # could be int or float, check for equality
        if i == f:
            return i
        else:
            return f



    def averageForGroup(self, df_name, group_name, col_to_avg):
        return self.data_frames[df_name].groupby(group_name).agg({col_to_avg:{'Average':'mean'}})

