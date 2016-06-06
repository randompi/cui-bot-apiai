# -*- coding: utf-8 -*-

import mock
import unittest
import json

import bot.sqlyzer as sqlyzer

class TestSqlyzer(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.sqlyzer = sqlyzer.Sqlyzer(sqlyzer.billing_code_schema[0])

    def tearDown(self):
        del self.sqlyzer

    def testInit(self):
        expected_tables = {
            'BillingCodes' : {
                'columns': [
                    {'name':'BillingCodeID'},
                    {'name': 'BillingCodeDesc'},
                    {'name': 'ImageModality'},
                    {'name': 'ContrastMaterial'},
                    {'name': 'StressImaging'},
                ],
            },
        }
        #print "\nAcutal: {}\n".format(self.sqlyzer.tables)
        self.assertDictEqual(expected_tables, self.sqlyzer.tables)