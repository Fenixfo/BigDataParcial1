import json
import requests
from bs4 import BeautifulSoup
import os

def lambda_handler(event, context):
    ### Download "publimetro" page
    p = requests.get('https://www.publimetro.co/co/%27)
    ### Download "el tiempo" page
    t = requests.get('https://www.eltiempo.com/%27)
    soup_p = BeautifulSoup(p.text, 'lxml')
    soup_t = BeautifulSoup(t.text, 'lxml')
    ###Write the file
    archivo=open('publimetro.html','w')
    archivo.write(str(soup_p))
    archivo.close()
    archivo=open('eltimpo.html','w')
    archivo.write(str(soup_t))
    archivo.close()

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }