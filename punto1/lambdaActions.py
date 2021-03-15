import json
### package to extract data from various internet sources
from pandas_datareader import DataReader
### will be working on temporary basis
import datetime as dt
import warnings
warnings.filterwarnings('ignore')

import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3

s3 = boto3.client('s3')
   
### Ccreate a time range (start and end variables)
### start=today
### end=yesterday

today = dt.datetime.today()
inicio = today - dt.timedelta(days=1)
day_actions = inicio.day
month_actions = inicio.month
year_actions = inicio.year

inicioMal = dt.datetime(2021,month_actions,1)

### Select the simbols Avianca, Ecopetrol, Grupo Aval & Cementos Argos in a variable called "acciones"
actions = ['AVHOQ','CMTOY','EC','AVAL']
### Select the simbols Avianca, Ecopetrol, Grupo Aval & Cementos Argos in a variable called by the name of avery one
cementos_argos = ['CMTOY']
grupo_aval = ['AVAL']
avianca = ['AVHOQ']
ecopetrol = ['EC']

### data is read from Yahoo and is assigned to Dataframe 'stocks'
cementos_argos = 'CMTOY'
grupo_aval = 'AVAL'
avianca = 'AVHOQ'
ecopetrol = 'EC'
stocks_cementos_argos = DataReader(cementos_argos,'yahoo',start=inicioMal)
stocks_cementos_argos = stocks_cementos_argos.iloc[-3:-1,:]
stocks_grupo_aval = DataReader(grupo_aval,'yahoo',start=inicio)
stocks_avianca = DataReader(avianca,'yahoo',start=inicio)
stocks_ecopetrol = DataReader(ecopetrol,'yahoo',start=inicio)
stocks_actions=[stocks_avianca,stocks_cementos_argos,stocks_ecopetrol,stocks_avianca]
### New Bucket
newBucket='parcialbigdata1'

for i in range(4):
    upload_path= newBucket+'/stocks/company='+actions[i]+'/year='+str(year_actions)+'/month='+str(month_actions)+'/day='+str(day_actions)+'/'+actions[i]+'.csv'
    ### Upload file
    stocks_actions[i].to_csv(actions[i]+'.csv')
    s3.upload_file(actions[i]+'.csv', newBucket, upload_path)








