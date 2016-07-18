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
        key = 'Billing_Codes'
        df = self.dms.data_frames[key]
        self.assertIsNotNone(df)


    def test_findRelevantFrames(self):
        #stream_handler = logging.StreamHandler(sys.stdout)
        #logger.addHandler(stream_handler)
        query_params = {'aggregable':'scan_time', 'groupable':'hospital'}
        expected = {'scan_time': [('Study_List', 'Scan_time', 0.8888888888888888)], 'hospital': [('Procedure_Revenue', 'Hospital_Name', 0.6666666666666666), ('Physician_List', 'Hospital_Name', 0.6666666666666666)]}

        result = self.dms.findRelevantFrames(query_params)

        self.assertEqual(result, expected)
        #logger.removeHandler(stream_handler)