# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 14:58:52 2019

@author: mazaror
"""

import pyodbc
import pandas as pd

def query_string(query_sentence):
    pyodbc.autocommit = True
    myconnection = pyodbc.connect('DSN=Impala', autocommit=True)
    data = pd.read_sql(query_sentence, myconnection)
    data.columns.names = ['index']
    myconnection.close()
    return data

def exec_string(sentence):
    pyodbc.autocommit = True
    myconnection = pyodbc.connect('DSN=Impala', autocommit=True)
    cur = myconnection.cursor()
    cur.execute(sentence)
    myconnection.commit()
    myconnection.close()


df = query_string(""" 
            select * from sod_onehouse.item_dim limit 10 
""")