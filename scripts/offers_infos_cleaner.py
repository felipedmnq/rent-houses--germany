'''Get the raw data from the DB, clean and organize it.

This script gets the raw dataset with all rent offers in the predefinated cities
in Germany, separate the meaningful information, clean it and organize it in 
different columns.

Returns a new dataframe read to be used.
'''
    
# imports
import re
import os
import requests
import psycopg2
import logging
import numpy as np
import pandas as pd
import psycopg2.extras as extras
from config import config
from get_offers_by_city import connect

# set log folder, files and object configs
if not os.path.exists('Logs'):
    os.makedirs('Logs')
    
logging.basicConfig(
    filename='Logs/offers_infos_cleaner.txt',
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m_%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('offers_infos_cleaner')


def get_data_from_db(conn):
    '''Get the latest number os offers for each city.
    
    Parameters:
    ----------
        conn: connection to the database to extract needed infos
        
    Return:
    -------
        Return all offer raw infos to be cleaned.   
    '''   
    
    # create a cursor object
    cursor = conn.cursor()
    
    # get infos from the database
    query = '''
        SELECT * FROM all_offers_raw_infos
    '''
    
    cursor.execute(query)
    result = cursor.fetchall()
    df_raw = pd.DataFrame(result, columns=['offer_id', 'extraction_date', 'city', 'city_code', 'offer_type', 'lat_lng', 'offer_infos'])
    
    return df_raw


def offers_infos_cleaning(df_raw):
    '''Clean and separate meaningful infos
    
    Parameter:
    ----------
        df_raw: Dataframe to be cleaned
        
    Return:
    -------
        Returns a new dataframe with the meaningful informations separated by columns
        and cleaned.    
    '''
    # Separate into latitude (lat) and longitude (lng)
    df_raw['lat'] = df_raw['lat_lng'].apply(lambda x: re.findall('\d+.\d+', x)[0])
    df_raw['lng'] = df_raw['lat_lng'].apply(lambda x: re.findall('\d+.\d+', x)[1])

    # drop original lat_lng column
    df_raw.drop(columns='lat_lng', inplace=True)

    df_list = []

    for x in range(len(df_raw)):
        infos_dict = {}  
        
        # get infos from df_raw
        infos_dict['offer_id'] = df_raw['offer_id'][x]
        infos_dict['extraction_date'] = df_raw['extraction_date'][x]
        infos_dict['lat'] = df_raw['lat'][x]
        infos_dict['lng'] = df_raw['lng'][x]

        # preprocess the infos cell
        b = df_raw['offer_infos'][x].replace('\\', '')
        b = b.replace('{', '').replace('}', '')[4:]
        b = b[:-1]
        b = b.split(',')
        
        # get all meaningful infos and return it cleane and
        # separated by columns.
        for i in b:
            # offer area
            if 'area' in i:
                try:
                    i = i.replace('"', '').replace("'", "").replace('area:', '').replace(' ', '')
                    infos_dict['area_m2'] = float(i)
                except:
                    infos_dict['area_m2'] = np.nan
                    logger.debug(f'Offer {i} has no information about area.')
            # if the offer is furnished or not
            if 'mobex' in i:
                if 'true' in i:
                    infos_dict['furnished'] = 1
                elif 'false' in i:
                    infos_dict['furnished'] = 0
                else:
                    infos_dict['furnished'] = np.nan
                    logger.debug(f'Offer {i} has no information about furniture.')
            #else:
            #    infos_dict['furnished'] = np.nan
            #    logger.debug(f'Offer {i} has no information about furniture.')
            # the offer zip code 
            if 'zip' in i:
                try:
                    infos_dict['zip_code'] = int(re.findall('\d+', i)[0])
                except:
                    infos_dict['zip_code'] = np.nan
                    logger.debug(f'Offer {i} has no information about zip_code.')
            # offer category
            if 'objectcat' in i:
                try:
                    infos_dict['main_category'] = re.findall('\:"\w+"', b[3])[0][1:].replace('"', '')
                except:
                    infos_dict['main_category'] = np.nan
                    logger.debug(f'Offer {i} has no information about main category.')
            # number of rooms
            if 'rooms' in i:
                try:
                    infos_dict['rooms'] = float(re.findall('\d+', i)[0])
                except:
                    infos_dict['rooms'] = np.nan
                    logger.debug(f'Offer {i} has no information about number of rooms.')
            # build yuear of construction
            if 'buildyear' in i:
                try:
                    infos_dict['build_year'] = int(re.findall('\d+', i)[0])
                except:
                    infos_dict['build_year'] = np.nan
                    logger.debug(f'Offer {i} has no information about build construction year.')
            # state
            if 'fed' in i:
                try:
                    infos_dict['state'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['state'] = np.nan
                    logger.debug(f'Offer {i} has no information about state.')
            # city
            if 'city' in i:
                try:
                    infos_dict['city'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['city'] = np.nan
                    logger.debug(f'Offer {i} has no information about city.')
            # offer sub-category
            if 'obcat' in i:
                try:
                    infos_dict['sub_category'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['sub_category'] = np.nan
                    logger.debug(f'Offer {i} has no information about sub-category.')
            # if the offer has or not a "balcon"- balcony
            if 'balcn' in i:
                if 'true' in i:
                    infos_dict['balcony'] = 1
                elif 'false' in i:
                    infos_dict['balcony'] = 0
                else:
                    infos_dict['balcony'] = np.nan
                    logger.debug(f'Offer {i} has no information about balcony.')
            #else:
            #    infos_dict['balcony'] = np.nan
            #    logger.debug(f'Offer {i} has no information about balcony.')
            # heat type
            if 'heatr' in i:
                try:
                    infos_dict['heat_type'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['heat_type'] = np.nan
                    logger.debug(f'Offer {i} has no information about heat type.')
            # offer title
            if 'title' in i:
                try:
                    infos_dict['offer_title'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['offer_title'] = np.nan
                    logger.debug(f'Offer {i} has no information about offer title.')
            # if the offer has already a kitchen
            if 'kitch' in i:
                if 'true' in i:
                    infos_dict['kitchen'] = 1
                elif 'false' in i:
                    infos_dict['kitchen'] = 0
                else:
                    infos_dict['kitchen'] = np.nan
                    logger.debug(f'Offer {i} has no information about kitchen.')
            #else:
            #    infos_dict['kitchen'] = np.nan
            #    logger.debug(f'Offer {i} has no information about kitchen.')
            if 'gardn' in i:
                if 'true' in i:
                    infos_dict['garden'] = 1
                elif 'false' in i:
                    infos_dict['garden'] = 0
                else:
                    infos_dict['garden'] = np.nan
                    logger.debug(f'Offer {i} has no information about garden.')
            #else:
            #    infos_dict['garden'] = np.nan
            #    logger.debug(f'Offer {i} has no information about garden.')
            # offer rent price
            if 'price' in i:
                try:
                    infos_dict['rent_price'] = float(re.findall('\d+', i)[0])
                except:
                    infos_dict['rent_price'] = np.nan
                    logger.debug(f'Offer {i} has no information rent price.')
                    
        # append the infos about the offer           
        df_list.append(infos_dict)
        logger.info(f'Offer no. {i} cleaned.')
        
    # create a new cleaned dataframe
    df_cleaned = pd.DataFrame(df_list)

    return df_cleaned

def load_df_cleaned(df_cleaned, conn):
    '''Get the informations and store in a database
    
    Params:
    -------
        df_infos: dataframe to be stored.
        conn: connection to the database.
    Return:
    -------
        None

    '''
    table_name = 'rent_infos_cleaned'
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
        offer_id NUMERIC, 
        extraction_date TIMESTAMP, 
        lat DOUBLE PRECISION, 
        lng DOUBLE PRECISION, 
        area_m2 REAL, 
        furnished NUMERIC,
        zip_code NUMERIC, 
        main_category VARCHAR(100), 
        rooms REAL, 
        build_year NUMERIC, 
        state VARCHAR(100), 
        city VARCHAR(100),
        sub_category VARCHAR(100), 
        balcony NUMERIC, 
        heat_type VARCHAR(100), 
        offer_title TEXT, 
        kitchen NUMERIC,
        rent_price REAL, 
        garden NUMERIC)'''
    try:
        cursor.execute(query2)
        conn.commit()
        logger.info('New table created')
        print(f'Recreated {table_name}.')
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 2
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df_cleaned.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df_cleaned.columns))
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

def main():
    # connection to database
    conn = connect(config())
    
    df_raw = get_data_from_db(conn)
    df_cleaned = offers_infos_cleaning(df_raw)
    load_df_cleaned(df_cleaned, conn)
    
    conn.close()

if __name__=='__main__':
    main()