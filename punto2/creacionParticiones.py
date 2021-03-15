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
   
### Create a time of today
today = dt.datetime.today()
day_actions = today.day
month_actions = today.month
year_actions = today.year

### New Bucket
newBucket='parcialbigdata1-2'

client = boto3.client('athena', region_name='us-east-1')

paper=['eltiempo','publimetro']


for i in range(len(paper)):

    ### creation of parameters for Athena
    params = {
        'region': 'us-east-1',
        'database': 'noticia',
        'bucket': 'parcialbigdata1-2',
        'path': '/headlines/raw/',
        'query': 'alter table periodicos add partition(periodico="{}",year="{}",month="{}",day="{}");'.format(paper[i],year_actions,month_actions,day_actions)
    }

    ### creation of queryfor Athena
    response_query_execution_id = client.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    
    )
    
    ### query response details
    response_get_query_details = client.get_query_execution(
        QueryExecutionId = response_query_execution_id['QueryExecutionId']
    )

    print("Result Data: "+paper[i])

