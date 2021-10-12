import os
import re
import math
import requests
import psycopg2
import numpy as np
import pandas as pd

from time import sleep
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine
from config import config

# get main German cities codes

def get_offers_qtt():
    '''
    Get offers quantity by city and store it in a postgres DB.
    '''
    
    cities_dict = {
        'Dusseldorf': 100207,
        'Berlin': 87372,
        'Essen': 102157,
        'Munchen': 121673,
        'Koln': 113144,
        'Stuttgart': 143262,
        'Dresden': 100051,
        'Hannover': 109489,
        'Dortmund': 99990,
        'Frankfurt am Main': 105043,
        'Hamburg': 109447
    }
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now2 = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    # get all offers quantity - haus und wohnung

    cities_offers = []

    for city, code in cities_dict.items():

        total_offers = 0

        for i in range(1,3):
            url = f'https://www.immonet.de/immobiliensuche/sel.do?parentcat={i}&objecttype=1&pageoffset=378&listsize=27&suchart=1&sortby=0&city={code}&marketingtype=2&page=1'
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, "html.parser")

            # Find number of rent offers.
            a = soup.findAll('h1', class_='box-50')[0].get_text()
            total_offers += int(re.search('\d+', a).group())

        cities_offers.append({'extraction_datetime': now, 'city': city, 'city_code': code, 'offers': total_offers})

    # offers_by_page = len(soup.findAll('div', class_="col-xs-12 place-over-understitial sel-bg-gray-lighter"))    
    df_offers = pd.DataFrame(cities_offers)
    #df_offers.to_csv(f'../data/total_offers_by_city_{now2}.csv', index=False)
    
    return df_offers


def connect():
    """ 
    Connect to the PostgreSQL database server 
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**config())
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    
    return conn



    
    