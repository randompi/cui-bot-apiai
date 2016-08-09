# -*- coding: utf-8 -*-

import logging
import mock
import unittest
import sys
import os
import json

import bot.data_models as dm

logger = logging.getLogger()
logger.level = logging.DEBUG

class TestDataModels(unittest.TestCase):
    def setUp(self):
        self.dms = dm.DataModels()

    def tearDown(self):
        del self.dms


    def test_getDataFrameByKey(self):
        key = 'Study_List'
        df = self.dms.data_frames[key]
        self.assertIsNotNone(df)


    def test_findRelevantFrames(self):
        #stream_handler = logging.StreamHandler(sys.stdout)
        #logger.addHandler(stream_handler)
        query_params = {'aggregable':'scan_time', 'groupable':'hospital'}
        expected = {'scan_time': [('Study_List', 'Scan_time', 0.8888888888888888)], 'hospital': [('Procedure_Revenue', 'Hospital_Name', 0.6666666666666666)]}

        result = self.dms.findRelevantFrames(query_params)

        self.assertEqual(result, expected)
        #logger.removeHandler(stream_handler)


    def test_map_to_frame(self):

        result = self.dms.map_to_frame('studies')
        self.assertEqual(result, 'Study_List')

        result = self.dms.map_to_frame('procedures')
        self.assertEqual(result, 'Procedure_Revenue')


    def test_map_to_frame_col(self):

        result = self.dms.map_to_frame_col('indication')
        self.assertEqual(result, 'Indication')

        result = self.dms.map_to_frame_col('CPT Codes')
        self.assertEqual(result, 'CPT_Code')

    def test_max_levenshtein_ratio(self):

        result = self.dms.max_levenshtein_ratio('', [])
        self.assertIsNone(result[0])
        self.assertEqual(result[1], 0.0)

        result = self.dms.max_levenshtein_ratio('cat', [])
        self.assertIsNone(result[0])
        self.assertEqual(result[1], 0.0)

        result = self.dms.max_levenshtein_ratio('cat', ['cat'])
        self.assertEqual(result[0], 'cat')
        self.assertEqual(result[1], 1.0)

        result = self.dms.max_levenshtein_ratio('cart', ['cArt'])
        self.assertEqual(result[0], 'cArt')
        self.assertEqual(result[1], 0.75)

        result = self.dms.max_levenshtein_ratio('cart', ['cArt', 'stArt'])
        self.assertEqual(result[0], 'cArt')
        self.assertEqual(result[1], 0.75)

        result = self.dms.max_levenshtein_ratio('cart', ['dog', 'cArt', 'stArt', 'fart'])
        self.assertEqual(result[0], 'cArt')
        self.assertEqual(result[1], 0.75)

        result = self.dms.max_levenshtein_ratio('c* _a+t', ['cArt', 's* _r+t'])
        self.assertEqual(result[0], 's* _r+t')
        self.assertAlmostEquals(result[1], 0.714285, 5)

    def test_generate_column_queries(self):

        result = self.dms.generate_column_queries(None)
        self.assertEqual(result, [])

        result = self.dms.generate_column_queries({})
        self.assertEqual(result, [])

        result = self.dms.generate_column_queries({'col1':'Scan Time'})
        expected = self.dms.ColQuery(col='Scan_time', filter=None, comp=None)
        self.assertEqual(result, [expected])

        in_params_map = {
            'col1': 'Scan Time',
            'filter1': 25,
        }
        result = self.dms.generate_column_queries(in_params_map)
        expected = self.dms.ColQuery(col='Scan_time', filter=25, comp='==')
        self.assertEqual(result, [expected])

        in_params_map = {
            'col1': 'Scan Time',
            'filter2': 'NA',
        }
        result = self.dms.generate_column_queries(in_params_map)
        expected = self.dms.ColQuery(col='Scan_time', filter=None, comp=None)
        self.assertEqual(result, [expected])

        in_params_map = {
            'col1': 'Scan Time',
            'filter1': 25,
            'comparison-filter1': 'greater than'
        }
        result = self.dms.generate_column_queries(in_params_map)
        expected = self.dms.ColQuery(col='Scan_time', filter=25, comp='>')
        self.assertEqual(result, [expected])

        in_params_map = {
            'col1': 'Scan Time',
            'col2': 'indication',
            'filter1': 25.35,
            'filter2': 'HCM',
            'comparison-filter1': 'greater than',
        }
        result = self.dms.generate_column_queries(in_params_map)
        expected1 = self.dms.ColQuery(col='Scan_time', filter=25.35, comp='>')
        expected2 = self.dms.ColQuery(col='Indication', filter='HCM', comp='==')
        self.assertEqual(sorted(result), sorted([expected1, expected2]))

        in_params_map = {
            'col': 'gender',
            'col1': 'Scan Time',
            'col2': 'indication',
            'filter': 'M',
            'filter1': '25',
            'filter2': 'HCM',
            'comparison-filter1': 'greater than',
        }
        result = self.dms.generate_column_queries(in_params_map)
        expected1 = self.dms.ColQuery(col='Scan_time', filter=25, comp='>')
        expected2 = self.dms.ColQuery(col='Indication', filter='HCM', comp='==')
        expected3 = self.dms.ColQuery(col='Gender', filter='M', comp='==')
        self.assertEqual(sorted(result), sorted([expected1, expected2, expected3]))

