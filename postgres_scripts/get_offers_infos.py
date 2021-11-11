'''Get all rent offer infos

This script gest all rent offer infos from each predeterminated city.
It uses the ids from the rent offers and extract all the information 
from the offer and saves it in a row format in database table named
"all_offers_infos".

Each row contains all the rent offer information in a raw format, with 
no preprocessing. 
'''

import os
import re
import math
import requests
import psycopg2
import logging
import numpy as np
import pandas as pd

from tqdm import tqdm
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
    filename='Logs/get_offers_infos.txt',
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m_%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('get_offers_infos')


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
        SELECT * FROM all_ids
    '''
    
    cursor.execute(query)
    result = cursor.fetchall()
    #df = pd.DataFrame(result)
    df_ids = pd.DataFrame(result, columns=['extraction_datetime', 'offer_id', 'city', 'city_code', 'type'])
    
    return df_ids

# get all infor for all founded offers
def get_offers_infos(df_ids, save=True):
    '''Get all infos from all rent offers
    
    Params:
    -------
        df_ids: offers ids source.
        save: save the returned dataframe locally or not.
        
    Returns:
    --------
        A dataframe with all offer informations in a row format.
    '''
    
    infos_list = []
    count = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print('Start data extraction...')
    with tqdm(total=df_ids.shape[0]) as pbar:
        for Id in set(df_ids['offer_id']):

            url = f"https://www.immonet.de/angebot/{Id}?drop=sel&related=false&product=standard"
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, "html.parser")

            #panels_infos = soup.findAll('div', class_='row box-50')
            # get all infos about each offer (all infos are mixed)

            # get all infos in almost a json format
            script_infos = soup.select('script', type_='text/javascript')
            
            for i in script_infos:
                if 'targetingParams' in i.text:
                    c = i.text
                    infos = re.findall('\{(.*?)\}', c)
                
                # get lat/lng
                if 'initModalMap' in i.text:
                    try:
                        c = i.text.replace('\n', '').replace('\t', '')
                        lat_lng = re.findall('\{lat: \d+.\d+,lng: \d+.\d+}', c)
                    except:
                        lat_lng = np.nan
                        
            #offer_infos = []
            #for panel in panels_infos:
            #    text = panel.text.replace('\n', '').replace('\t', '-')
            #    offer_infos.append(text)



            infos_list.append({'offer_id': Id,
                               'extraction_date': now,
                               'city': df_ids['city'][count],
                               'city_code': df_ids['city_code'][count],
                               'offer_type': df_ids['type'][count],
                               'lat_lng': lat_lng,
                               'offer_infos': infos})
            sleep(1)
            pbar.update(1)
            #print(count)
            count += 1

    df_infos = pd.DataFrame(infos_list)
    df_infos.drop_duplicates(subset='offer_id', inplace=True)
    logger.info('df_infos created')
    print('Data extraction completed.')
    
    if save:
        if not os.path.exists('../data'):
            os.makedirs('../data')
        now2 = datetime.now().strftime('%Y_%m_%d')
        df_infos.to_csv(f'../data/df_infos_{now2}.csv', index=False)
        
    return df_infos


def load_offer_ids(df_infos, conn):
    '''Get the informations and store in a database
    
    Params:
    -------
        df_infos: dataframe to be stored.
        conn: connection to the database.
    Return:
    -------
        None

    '''
    table_name = 'all_offers_raw_infos'
    # delete table 
    query1 = f'DROP TABLE IF EXISTS {table_name}'
    cursor = conn.cursor()
    print('initiated...')
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
        offer_id INTEGER,
        extraction_date TEXT,
        city VARCHAR(50),
        city_code INTEGER,
        offer_type VARCHAR(50),
        lat_lng TEXT,
        offer_infos TEXT)'''
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
    tuples = [tuple(x) for x in df_infos.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df_infos.columns))
    # SQL quert to execute
    query3 = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
    try:
        extras.execute_values(cursor, query3, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        #logger.error(f"Error: {error}")
        conn.rollback()
        cursor.close()
        print(error)
        return 3
    logger.info(f"{table_name} uptodate.")
    print(f"{table_name} uptodate.")
    cursor.close()
    
    return None

def main():
    # connection to database
    conn = connect(config())
    
    df = get_data_from_db(conn)
    df_infos = get_offers_infos(df)
    load_offer_ids(df_infos, conn)
    
    conn.close()
    
if __name__=='__main__':
    main()