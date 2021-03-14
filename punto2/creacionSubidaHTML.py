import json
import os
import boto3
from urllib.parse import unquote_plus
s3 = boto3.client('s3')
### will be working on temporary basis
import datetime as dt


def lambda_handler(event, context):
    
    ### Get actual day
    today = dt.datetime.today()
    day_actual = today.day
    month_actual = today.month
    year_actual = today.year
    
    ### Get file path
    key=unquote_plus(event['Records'][0]['s3']['object']['key'])
    ### Get bucket name
    bucketName=unquote_plus(event['Records'][0]['s3']['bucket']['name'])
    ### Download path
    download_path = '/tmp/{}.'.format(key.split('/')[-1])
    ### Name file
    nameFile=key.split('/')[-1].split('.')[0]
    ### New Key
    newKey='headlines/raw/periodico='+nameFile+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+nameFile+'.html'

    ### Download file
    s3.download_file(bucketName,newKey,download_path)
    
    ### Upload path
    upload_path = 'news/raw/periodico='+nameFile+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+nameFile+'.html'
    ### Upload file
    s3.upload_file(download_path, bucketName, upload_path)

    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
