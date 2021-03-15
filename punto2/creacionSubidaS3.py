import json
import requests
from bs4 import BeautifulSoup
import os
import boto3
### will be working on temporary basis
import datetime as dt
s3 = boto3.client('s3')

### New Bucket
newBucket='parcialbigdata1-2' 

link_publimetro='https://www.publimetro.co/co/'
link_eltiempo='https://www.eltiempo.com'
name_publimetro='publimetro'
name_eltiempo='eltiempo'

### Download "publimetro" page
p = requests.get(link_publimetro)
### Download "el tiempo" page
t = requests.get(link_eltiempo)
soup_p = BeautifulSoup(p.text, 'lxml')
soup_t = BeautifulSoup(t.text, 'lxml')
### File creation
archivo=open(name_publimetro+'.html','w', encoding='utf-8')
archivo.write(str(soup_p))
archivo.close()
### File creation
archivo=open(name_eltiempo+'.html','w', encoding='utf-8')
archivo.write(str(soup_t))
archivo.close()

### Get actual day
today = dt.datetime.today()
day_actual = today.day
month_actual = today.month
year_actual = today.year

### Upload path
upload_path_publimetro = 'headlines/raw/periodico='+name_publimetro+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_publimetro+'.html'
upload_path_eltiempo = 'headlines/raw/periodico='+name_eltiempo+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_eltiempo+'.html'
### Upload file
s3.upload_file(name_publimetro+'.html', newBucket, upload_path_publimetro)
s3.upload_file(name_eltiempo+'.html', newBucket, upload_path_eltiempo)

