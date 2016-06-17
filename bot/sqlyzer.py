# -*- coding: utf-8 -*-

import logging
import sqlparse
import sqlparse.sql as sql
from sqlparse.tokens import Token

logger = logging.getLogger(__name__)

billing_code_schema = [
    '''CREATE TABLE BillingCodes (
        BillingCodeID INT NOT NULL,
        BillingCodeDesc VARCHAR(255),
        ImageModality ENUM('MRI','CT'),
        ContrastMaterial ENUM('Without', 'With', 'WithoutThenWith'),
        StressImaging BOOL,
        PRIMARY KEY (BillingCodeID)
    );'''
]

data_types = ['INT','int','VARCHAR','varchar','CHAR','char','ENUM','enum','BOOL','bool']

class Sqlyzer(object):

    def __init__(self, schema):
        self.tables = self._parse_schema(schema)

    def _parse_schema(self, schema):

        tables = {}
        parsql = sqlparse.parse(schema)
        for stmt in parsql:
            if stmt.get_type() == 'CREATE':
                if any(t for t in stmt.tokens if t.value == 'TABLE'):
                    for func in filter(lambda t: isinstance(t, sql.Function), stmt.tokens):
                        columns = []
                        for paren in filter(lambda t: isinstance(t, sql.Parenthesis), func.tokens):
                            paren_tokens = list(paren.flatten())
                            i = 0
                            while(i < len(paren_tokens) and paren_tokens[i].value != 'PRIMARY'):
                                #print 'Processing: {}'.format((paren_tokens[i].ttype, paren_tokens[i].value))
                                # looking for column declarations of the form
                                # <column_name> <column_type> <optional_constraints>*,
                                # e.g.:
                                # BillingCodeID INT NOT NULL,
                                # ImageModality ENUM('MRI','CT'),
                                if paren_tokens[i].ttype is Token.Name and \
                                                paren_tokens[i].value not in data_types:
                                    column = {}
                                    column['name'] = paren_tokens[i].value.encode('ascii', 'ignore')
                                    #TODO: Pull out data type info and any optional constraints
                                    columns.append(column)
                                    #print 'Added column: {}'.format(column)

                                i += 1
                            # end while

                        tables[func.get_name().encode('ascii', 'ignore')] = {'columns':columns}

        return tables

    def generate_select_stmt(self, intent_name, params):
        for table_name, table in self.tables.iteritems():
            if table.get('intent_name') == intent_name:
                where_clause = self._generate_where_clause(table_name, params)
                select_stmt = 'SELECT * FROM {} WHERE {};'.format(table_name, where_clause)
                logger.debug('select_stmt: {}'.format(select_stmt))
                return '```{}```\n'.format(select_stmt)

        return ''

    def _generate_where_clause(self, table_name, params):
        first_param = True
        where_clause = ''
        for param, param_val in params.iteritems():
            for col in self.tables.get(table_name).get('columns'):
                if col.get('entity') == param:
                    if first_param:
                        first_param = False
                    else:
                        where_clause += 'AND '
                    if isinstance(param_val, basestring):
                        param_val = "'{}'".format(param_val)
                    where_clause += col.get('name') + '=' + str(param_val) + ' '
                    break

        return where_clause
