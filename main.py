# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 12:07:11 2020

@author: Marco
"""

from Commons import one_house as OH
from sklearn import tree
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import StandardScaler
import pandas as pd
import matplotlib.pyplot as plt
from aux_functions import gen_data

# Load data
data = gen_data('2019-03-01','2019-05-01')

data.to_pickle('C:/Users/mazaror/Documents/Respaldo Maro Zaror/Personal/Python/Sodimac/test/tablon')
data = pd.read_pickle('C:/Users/mazaror/Documents/Respaldo Maro Zaror/Personal/Python/Sodimac/test/tablon')

#########################################
##        Data understanding           ##
#########################################

# General
data.info()
data.describe()

# Null checking
data.isnull().any()
data[data['total_mails'].isnull() == True]

#data2 = tablon.dropna(axis=0, subset=['total_mails'])

# Visualization
#data['flag_unsubs'] = pd.to_numeric(tablon['flag_unsubs']) # sunday = 1
res_dia = data[['dia_sem','flag_unsubs']].groupby(by='dia_sem').mean()

res_ult = data[['total_mails','flag_unsubs']].groupby(by='flag_unsubs').mean()
data['flag_unsubs'][data['var_open_15_7'] > 3].mean()
data['flag_unsubs'][data['var_open_15_7'] > 2].count()
data['flag_unsubs'].mean()

#########################################
##              ETL                    ##
#########################################

columns = ['flag_unsubs','cantidad_ult1','mar_mier','mier','may4_ult3','may7_ult7','var_cant_15_7_may3']
data2 = data[columns]

data.hist(column='cantidad_ult7', by='flag_unsubs')

data2.info()

# Standarization and scalling
scaler = StandardScaler()
data2 = scaler.fit_transform(data2)

# Processing
data2 = pd.get_dummies(data.iloc[:,-2:-1])
data3 = pd.concat([data,data2], axis=1)
del data, data2
data4 = data3.iloc[:,[1,5,6,7,8,9,10,11,13,14,15]]
del data3

#########################################
##           Modelling                 ##
#########################################


# Split data
train, test = train_test_split(data2, test_size=0.2)

# Subsampling (due to unbalance classes)
data_1 = train[train['flag_unsubs']==1]
data_0 = train[train['flag_unsubs']==0].sample(len(tablon_1))
data_tr = data_1.append(data_0)
del data_1,data_0

x = train.iloc[:,1:]
y = pd.DataFrame(train.iloc[:,0])
x_te = test.iloc[:,1:]
y_te = pd.DataFrame(test.iloc[:,0])

# Fitting different models

dt = tree.DecisionTreeClassifier()
dt = dt.fit(x,y)
pred = dt.predict(x_te)
matriz = confusion_matrix(y_te,pred)

lr = LogisticRegression()
lr = lr.fit(x,y)
pred_lr = lr.predict(x_te)
matriz = confusion_matrix(y_te,pred)

