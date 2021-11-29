import os
import argparse
import numpy as np
import pandas as pd
import sweetviz as sv


def de_rent_cleaner(path, sumary):
    '''Simple Cleaner for all_offers_infos_pp.csv
    
    Removes null values, possible outliers and convert types.
    '''
    
    df = pd.read_csv(path)
    rows_before = df.shape[0]
    columns_before = df.shape[1]
    # rent price
    if 'rent_price' in df.columns:
        # Exclude null values for rent price column - main information.
        df.dropna(subset=['rent_price'], inplace=True, axis=0)
        # Rend price under 200
        df = df[df['rent_price'] >= 200]
    else:
        pass
    # area
    if 'area_m2' in df.columns:
        # Exclude null values for area column.
        df.dropna(subset=['area_m2'], inplace=True, axis=0)
        # Remove area under 15m2
        df = df[df['area_m2'] >= 15]
    else:
        pass
    # extraction date
    if df['extraction_date'].dtype != 'O': # check if the type is already datetime
        # Convert extraction date to datetime.
        df['extraction_date'] = pd.to_datetime(df['extraction_date'])
    else:
        pass
    #rooms
    if 'rooms' in df.columns:
        # exclude offers with too many rooms
        df = df[df['rooms'] < 30]
    else:
        pass
    # build year
    if 'build_year' in df.columns:
        # build year < 1000 and > 2021
        df['build_year'] = df['build_year'].apply(lambda x: np.nan if (x < 1000 or x > 2021) else x)
    else:
        pass
    # furnished
    if 'furnished' in df.columns:
        if len(df['furnished'] != 2):
            try:
                df.drop(columns=['furnished'], inplace=True)
            except:
                pass
        else:
            pass
    else:
        pass
    
    df_cleaned = df
    
    rows_after = df.shape[0]
    columns_after = df.shape[1]
    
    if sumary:
        print(f'Cleaning summary:\n\nExcluded rows: {rows_before-rows_after}\nExcluded columns: {columns_before-columns_after}\n')
    
    return df_cleaned

def parse_args():
    
    parser = argparse.ArgumentParser(description='Simple Cleaner for all_offers_infos_pp.csv')
    
    parser.add_argument(
        "-p",
        "--filepath",
        type=str,
        default="../data/all_offers_infos_pp.csv",
        help="Add all_offers_infos_pp.csv path",
    )
    
    parser.add_argument(
        "-s",
        "--sumary",
        type=bool,
        default=False,
        help="Print a summary of the cleaning process",
        choices=[True,False]
    )
    
    args = parser.parse_args()
    return args
                
def main():
    
    args = parse_args()
    path = args.filepath
    summary_or_not = args.summary
    df_cleaned = de_rent_cleaner(path=path,
                                 summary=summary_or_not)
    
    if not os.path.exists('../data'):
        os.makedirs('../data')
   
    output_dir = '../data/'
    filename = 'all_offers_cleaned.csv'
    df_cleaned.to_csv(os.path.join(output_dir, filename), index=False)
    print(f"{filename} saved at {output_dir}.")
    
if __name__=='__main__':
    main()
