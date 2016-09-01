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