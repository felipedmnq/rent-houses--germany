'''Get the ids from all rent offers in all determinated cities.

This script gets all the ids from all rent offers in all the predeterminated cities
and save it in a database table named "all_offer_ids" to further use.

The previous table is dropped and a new table is created each time that it runs.
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

from time import sleep
from config import config
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2.extras as extras
from sqlalchemy import create_engine
from get_offers_by_city import connect

# Create log folder if not exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')
    
logging.basicConfig(
    filename='Logs/get_offer_ids.txt',
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m_%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('get_offer_ids')


def get_data_from_db(conn):
    '''Get the latest number os offers for each city.
    
    Parameters:
    ----------
        conn: connection to the database to extract the infos
        
    Return:
    -------
        Return a dataframe with the latest data about the number of offers in each city.
        
    '''   
    
    # create a cursor object
    cursor = conn.cursor()
    
    # get infos from the database
    query = '''
        SELECT * FROM offers_qtt_by_city ORDER BY extraction_datetime DESC LIMIT 11
    '''
    
    cursor.execute(query)
    result = cursor.fetchall()
    #df = pd.DataFrame(result)
    df = pd.DataFrame(result, columns=['extraction_datetime', 'city', 'city_code', 'offers'])
    
    # get only the latest values
    #df = df.tail(11)
    df.reset_index(inplace=True, drop=True)
    
    return df

def get_offer_ids(df, save=False):
    '''Get all offer ids for each offer in each city
    
    Parameters:
    -----------
        df: a dataframe with the number of offers in each city.
        
        save: default=False
            save the returned dataframe locally or not.
            
    Return:
    -------
        Return a dataframe with all offer ids for each city with the type of the offer (wohnung/haus).
    '''    
    
    ids_list = []
    offers_by_page = 26

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for c in range(len(df)):
        city = df.loc[c]['city']
        city_code = df.loc[c]['city_code']
        offers = df.loc[c]['offers']
        
        print(f'Getting offer ids for {city}...')
        logger.info(f'Getting offer ids for {city}...')

        # get all offers ids for haus und wohnung in each city


        # Get the number of pages to scrape - rounded to down
        number_of_pages = math.floor(offers / offers_by_page)

        # wohnung/haus code
        l_opt = [1, 2]

        for opt in l_opt:
            for page in range(number_of_pages):
                url = f"https://www.immonet.de/immobiliensuche/sel.do?parentcat={opt}&objecttype=1&pageoffset=1&listsize=26&suchart=1&sortby=0&city={city_code}&marketingtype=2&page={page}"
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                page = requests.get(url, headers=headers)
                soup = BeautifulSoup(page.text, "html.parser")

                offers_list_1page = soup.findAll('div', class_="col-xs-12 place-over-understitial sel-bg-gray-lighter")
                
                for i in range(len(offers_list_1page)):
                    try:
                        if opt == 1:
                            ids_list.append({'extraction_datetime': now, 'offer_id': offers_list_1page[i].find('a')['data-object-id'], 'city': city, 'city_code': city_code, 'type': 'wohnung'})
                        if opt == 2:
                            ids_list.append({'extraction_datetime': now, 'offer_id': offers_list_1page[i].find('a')['data-object-id'], 'city': city, 'city_code': city_code, 'type': 'haus'})
                    except:
                        logger.error(f'Error - id:{i}')
                        pass
                sleep(randint(1, 2))         
        sleep(randint(1, 5))          

    # Create a dataframe with the infos
    df_ids = pd.DataFrame(ids_list)
    df_ids.drop_duplicates(subset='offer_id', inplace=True)

    # save as csv file
    if save:
        if not os.path.exists('../data'):
            os.makedirs('../data')
        output_dir = '../data/'
        now2 = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        filename = f'immonet_de_{now2}.csv'
        df_ids.to_csv(os.path.join(output_dir, filename), index=False)
    
    return df_ids

def load_offer_ids(df_ids, conn):
    table_name = 'all_offer_ids'
    # delete table 
    query1 = f'DROP TABLE IF EXISTS {table_name}'
    cursor = conn.cursor()
    try:
        cursor.execute(query1)
        conn.commit()
        logger.info('Old table droped')
        print(f'Deleted {table_name} table.')
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    query2 = f'''CREATE TABLE IF NOT EXISTS {table_name} (
        extraction_datetime TIMESTAMP,
        offer_id INTEGER,
        city VARCHAR(50),
        city_code INTEGER,
        type VARCHAR(50)
    )'''
    try:
        cursor.execute(query2)
        conn.commit()
        logger.info('New table created')
        print('Recreated all_offer_ids table.')
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 2
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df_ids.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df_ids.columns))
    # SQL quert to execute
    query3  = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
    try:
        extras.execute_values(cursor, query3, tuples)
        conn.commit()
        logger.info(f'{table_name} uptodate')
        print(f'{table_name} table is uptodate.')
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 3
    logger.info(f"{table_name} uptodate.")
    print(f"{table_name} uptodate.")
    cursor.close()
    
    return None
    
def main():
    # connection to database
    conn = connect(config())
    
    df = get_data_from_db(conn)
    df_ids = get_offer_ids(df)
    load_offer_ids(df_ids, conn)
    
    conn.close()
    
if __name__=='__main__':
    main()