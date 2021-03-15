import json
from pandas_datareader import DataReader
import datetime as dt
import warnings
warnings.filterwarnings('ignore')
import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3
s3 = boto3.client('s3')
today = dt.datetime.today()
inicio = today - dt.timedelta(days=1)
day_actions = inicio.day
month_actions = inicio.month
year_actions = inicio.year
inicioMal = dt.datetime(2021,month_actions,1)
actions = ['AVHOQ','CMTOY','EC','AVAL']
cementos_argos = ['CMTOY']
grupo_aval = ['AVAL']
avianca = ['AVHOQ']
ecopetrol = ['EC']
cementos_argos = 'CMTOY'
grupo_aval = 'AVAL'
avianca = 'AVHOQ'
ecopetrol = 'EC'
stocks_cementos_argos = DataReader(cementos_argos,'yahoo',start=inicioMal)
stocks_cementos_argos = stocks_cementos_argos.iloc[-2:-1,:]
stocks_grupo_aval = DataReader(grupo_aval,'yahoo',start=inicio)
stocks_avianca = DataReader(avianca,'yahoo',start=inicio)
stocks_ecopetrol = DataReader(ecopetrol,'yahoo',start=inicio)
stocks_actions=[stocks_avianca,stocks_cementos_argos,stocks_ecopetrol,stocks_avianca]
newBucket='parcial1-bigdata2021'
client = boto3.client('athena', region_name='us-east-1')
for i in range(4):
    upload_path = newBucket+'/stocks/company='+actions[i]+'/year='+str(year_actions)+'/month='+str(month_actions)+'/day='+str(day_actions)+'/'+actions[i]+'.csv'
    stocks_actions[i].to_csv(actions[i]+'.csv')
    s3.upload_file(actions[i]+'.csv', newBucket, upload_path)
    params = {
        'region': 'us-east-1',
        'database': parcial1',
        'bucket': 'parcial1-bigdata2021',
        'path': '/parcialbigdata1/stocks/',
        'query': 'alter table actions add partition(company="{}",year="{}",month="{}",day="{}");'.format(actions[i],year_actions,month_actions,day_actions)
    }
    response_query_execution_id = client.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        } 
    )
    response_get_query_details = client.get_query_execution(
        QueryExecutionId = response_query_execution_id['QueryExecutionId']
    )
    print("Result Data: "+actions[i])