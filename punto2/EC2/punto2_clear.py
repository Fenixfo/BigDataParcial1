import json
import requests
from bs4 import BeautifulSoup
import os
import boto3
import datetime as dt
s3 = boto3.client('s3')

newBucket='parcial1-bigdata2021' 

link_publimetro='https://www.publimetro.co/co/'
link_eltiempo='https://www.eltiempo.com'
name_publimetro='publimetro'
name_eltiempo='eltiempo'
p = requests.get(link_publimetro)
t = requests.get(link_eltiempo)
soup_p = BeautifulSoup(p.text, 'lxml')
soup_t = BeautifulSoup(t.text, 'lxml')

archivo=open(name_publimetro+'.html','w', encoding='utf-8')
archivo.write(str(soup_p))
archivo.close()

archivo=open(name_eltiempo+'.html','w', encoding='utf-8')
archivo.write(str(soup_t))
archivo.close()

today = dt.datetime.today()
day_actual = today.day
month_actual = today.month
year_actual = today.year

upload_path_publimetro = 'punto2/headlines/raw/periodico='+name_publimetro+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_publimetro+'.html'
upload_path_eltiempo = 'punto2/headlines/raw/periodico='+name_eltiempo+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_eltiempo+'.html'
s3.upload_file(name_publimetro+'.html', newBucket, upload_path_publimetro)
s3.upload_file(name_eltiempo+'.html', newBucket, upload_path_eltiempo)