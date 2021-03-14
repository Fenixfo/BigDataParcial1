import json
import os
import boto3
from urllib.parse import unquote_plus
s3 = boto3.client('s3')
### will be working on temporary basis
import datetime as dt


def lambda_handler(event, context):
    
    ### Get file path
    key=unquote_plus(event['Records'][0]['s3']['object']['key'])
    ### Get bucket name
    bucketName=unquote_plus(event['Records'][0]['s3']['bucket']['name'])
    ### Download path
    download_path = '/tmp/{}.'.format(key.split('/')[-1])
    ### Download file
    s3.download_file(bucketName,key,download_path)
    
    ### file modification
    fileInfo= open(download_path,'r')
    lines = open(download_path,'r').readlines()
    out = open(download_path,'w')
    out.writelines(lines)
    out.close()
    ### Category
    categoryFile=''
    for i in lines:
        if 'category' in i:
            
            #print(i.split('category')[1])
            if i.split('category')[1].split(' ')[0] != '':
                categoryFile=i.split('category')[1].split(' ')[0]
            else:
                categoryFile=i.split('category')[1].split(' ')[1]
            
            
            break
    ### Headline
    headLineFile=''
    for i in lines:
        if '<title>' in i:
            headLineFile=i.split('<title>')[1].split('</title>')[0].replace(',',' ')
            break
    
    ### link
    linkFile=''
    for i in lines:
        if 'canonical' in i:
            blockInfo=i.split(' ')
            for j in blockInfo:
                if 'href="https://www' in j:
                    linkFile=j.split('"')[1]
                    break
            break
        
    ### file modification
    archivo=open('/tmp/info.txt','w') 
    archivo.write('category,headline,link\n{},{},{}'.format(categoryFile,headLineFile,linkFile))
    archivo.close()
    ### Get actual day
    today = dt.datetime.today()
    day_actual = today.day
    month_actual = today.month
    year_actual = today.year

    ### Upload path
    upload_path = 'headlines/final/periodico='+linkFile.split('/')[2].split('.')[1]+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+linkFile.split('/')[2].split('.')[1]+'.csv'
    ### Upload file
    s3.upload_file('/tmp/info.txt', bucketName, upload_path)

    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
