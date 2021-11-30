'''Get quantity of rent offers by city

This script gets the quantity off all rent offers from each default city
and saves it in a postgres database, into the "offers_qtt_by_city" table,
to further use.

The "offers_qtt_by_city" table is appended with the new information to
historical record.

'''

import os
import re
import sys
import math
import requests
import psycopg2
import logging
import numpy as np
import pandas as pd
import psycopg2.extras as extras

from tqdm import tqdm
from time import sleep
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine
from config import config

if not os.path.exists('Logs'):
    os.makedirs('Logs')

logging.basicConfig(
    filename='Logs/get_offers_by_city.txt', 
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('get_offers_by_city')

def connect(conf):
    """ 
    Connect to the PostgreSQL database server 
    
    Parameters:
    ----------
        conf: configuration for connection
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        logger.info('Connecting to database...')
        print('Connecting to database...')
        conn = psycopg2.connect(**conf)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error('Connection to database failed')
        print(error)
        sys.exit(1)
    logger.info("Connection successful")    
    print("Connection successful")
    
    return conn

def get_offers_qtt(save=True):
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
        
        logging.info(f'Getting offers in {city}...')
        print(f'Getting offers in {city}...')

        total_offers = 0

        for i in range(1,3):
            url = f'https://www.immonet.de/immobiliensuche/sel.do?&sortby=0&suchart=1&objecttype=1&marketingtype=2&parentcat={i}&city={code}'
            #url = f'https://www.immonet.de/immobiliensuche/sel.do?parentcat={i}&objecttype=1&pageoffset=378&listsize=27&suchart=1&sortby=0&city={code}&marketingtype=2&page=1'
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, "html.parser")

            # Find number of rent offers.
            a = soup.select('ul', class_='tbl margin-auto margin-top-0 margin-bottom-0 padding-0')
            
            for i in a:
                if 'Alle Orte' in i.text:
                    c = i.text
                    total_offers += int(re.findall('\d+', c)[0])
            #total_offers += int(re.search('\d+', a).group())

        cities_offers.append({'extraction_datetime': now, 'city': city, 'city_code': code, 'offers': total_offers})

    # offers_by_page = len(soup.findAll('div', class_="col-xs-12 place-over-understitial sel-bg-gray-lighter"))    
    df_offers = pd.DataFrame(cities_offers)
    
    if save:
        if not os.path.exists('../data'):
            os.makedirs('../data')
        output_dir = '../data/'
        #now2 = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        filename = f'offers_qtt_by_city.csv'
        df_offers.to_csv(os.path.join(output_dir, filename), index=False)
    
    return df_offers


def load_results(conn, df, table_name):
    """Get the extracted data and insert into a postgres table.
    
    Parameters:
    -----------
        conn: connection to the database.
        df: Dataframe to insert into the database.
        table_name: table to insert the dataframe. 
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    logger.info(f"{table_name} uptodate.")
    print(f"{table_name} uptodate.")
    cursor.close()
    
def main():
    conn = connect(conf=config())
    df = get_offers_qtt()
    load_results(conn, df, table_name='offers_qtt_by_city')
    conn.close()
    
if __name__ == '__main__':
    main()

    
    