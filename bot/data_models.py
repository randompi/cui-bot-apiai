# -*- coding: utf-8 -*-

import pandas as pd

class DataModels(object):

    def __init__(self):
        self.data_frames = {}
        self.data_frames['Billing_Codes'] = pd.read_csv('resources/MedData_SQL_DB/Billing_Codes.csv')
        self.data_frames['Patient_Data'] = pd.read_csv('resources/MedData_SQL_DB/Patient_Data.csv')
        self.data_frames['Physician_List'] = pd.read_csv('resources/MedData_SQL_DB/Physician_List.csv')
        self.data_frames['Procedure_Revenue'] = pd.read_csv('resources/MedData_SQL_DB/Procedure_Revenue.csv')
        self.data_frames['Study_List'] = pd.read_csv('resources/MedData_SQL_DB/Study_List.csv')

    def averageForGroup(self, df_name, group_name, col_to_avg):
        return self.data_frames[df_name].groupby(group_name).agg({col_to_avg:{'Average':'mean'}})
