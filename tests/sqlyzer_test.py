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

    def test_generate_select_stmt(self):
        self.sqlyzer.tables = {'BillingCodes': {'intent_name': u'Find a Billing Code', 'columns': [{'name': 'BillingCodeID'}, {'name': 'BillingCodeDesc'}, {'name': 'ImageModality', 'entity': 'image_modality'}, {'name': 'ContrastMaterial', 'entity': 'contrast_use'}, {'name': 'StressImaging', 'entity': 'stress_use'}]}}

        intent_name = u'Find a Billing Code'
        params = {'stress_use': False, 'image_modality': u'MRI', 'contrast_use': 'with'}

        result_select_stmt = self.sqlyzer.generate_select_stmt(intent_name, params)
        expected_select_stmt = "SELECT * FROM BillingCodes WHERE {};".format(self.sqlyzer._generate_where_clause('BillingCodes', params))
        expected_select_stmt = '```{}```\n'.format(expected_select_stmt)

        self.assertEqual(result_select_stmt, expected_select_stmt)