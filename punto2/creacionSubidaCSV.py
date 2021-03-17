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
    
    sendPersonalCSV=''
    ### Category
    categoryFile=''
    ### Headline
    headLineFile=''
    ### link
    linkFile=''
    
    ### link
    linkFilePrincipal=''
    ### Get principal link
    for i in lines:
        if 'canonical' in i:
            blockInfo=i.split(' ')
            for j in blockInfo:
                if 'href="https://www' in j:
                    linkFilePrincipal=j.split('"')[1]
                    break
            break
    
    actualIndex=0
    for i in lines:
        if '<a class="category page-link' in i:
            try:
                theLine = i
                ### Category
                categoryFile = theLine.split('"')[1].split(' ')[2].replace(',',' ')
                ### link
                linkFile = lines[actualIndex+2].split('href="')[1].split('"')[0].replace(',',' ')
                if not ('www.' in linkFile or 'http' in linkFile):
                   linkFile = linkFilePrincipal + linkFile  
                ### Headline
                headLineFile = lines[actualIndex+2].split('</a>')[0].split('>')[-1].replace(',',' ')
                ### CSV
                sendPersonalCSV = sendPersonalCSV+'{}, {}, {} \n'.format(categoryFile,headLineFile,linkFile)
                
            except:
                print('Error category')
        actualIndex+=1
                
    for i in lines:
        if '"category":[{' in i:
            superLine = i.split('{"galery":')
            for k in superLine:
                try:
                    theLine = k
                    ### Category
                    categoryFile = theLine.split('"slug":"')[1].split('"')[0].replace(',',' ')
                    ### link
                    linkFile = theLine.split('"link":"')[1].split('"')[0].replace(',',' ')
                    if not 'www.' in linkFile:
                        linkFile = linkFilePrincipal + linkFile  
                    ### Headline
                    headLineFile = theLine.split('"title":{"rendered":"')[1].split('"')[0].replace(',',' ')
                    ### CSV
                    sendPersonalCSV = sendPersonalCSV+'{}, {}, {} \n'.format(categoryFile,headLineFile,linkFile)
                    
                except:
                    print('Error category')
    
    namePage = linkFilePrincipal.split('/')[2].split('.')[1]
    ### file modification
    archivo=open('/tmp/info.txt','w') 
    archivo.write(''+sendPersonalCSV)
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
