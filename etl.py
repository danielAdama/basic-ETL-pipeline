import pandas as pd
import numpy as np
import config
import requests
import sqlite3
import time
from tqdm.auto import tqdm
pd.set_option('display.max_columns', None)

# -----------------------------------------------EXTRACT--------------------------------------------------
# Fetch data from John Hopkins University's GitHub
download_urls = []
def extract(URL):
    """Function to extract data (csv) from the John Hopkins 
    University's repository
    """
    response = requests.get(URL)
    for csv_file in tqdm(response.json()):
        if csv_file['name'].endswith('csv'):
            download_urls.append(csv_file['download_url'])


# --------------------------------------------------TRANSFORM---------------------------------------------
# Some columns are inconsistent, rename those columns.
replace_name = {
    'Last Update': 'Last_Update',
    'Lat':'Latitude',
    'Long_':'Longitude',
    'Province/State':'Province_State',
    'Country/Region':'Country_Region',
    'Case-Fatality_Ratio':'Case_Fatality_Ratio'
}


def transform(df):
    """Function to relabel columns to maintain consistency and 
    keep_cols in the dataframe.
    """
    for col_name in df:
        if col_name in replace_name:
            df = df.rename(columns = replace_name)

    # if 'Last_Update' in df:
    #     df['Last_Update'] = pd.to_datetime(df['Last_Update'])
            
    # Return only these set of columns
    keep_cols = [
        'Province_State', 'Country_Region', 'Latitude', 'Longitude', 'Last_Update',
        'Confirmed', 'Deaths', 'Recovered'
    ]

    # Replace columns not in the dataframe with nan
    for col in keep_cols:
        if col not in df:
            df[col] = np.nan
    
    return df[keep_cols]

# --------------------------------------------------Load--------------------------------------------------
# We need to load the data into an SQL table
def load(db_name, file_urls, table_name):
    """Function to Store all data in a single table in an SQL database
    """
    con = sqlite3.connect(f"{db_name}.db")
    for i, file_path in enumerate(file_urls):
        data = pd.read_csv(file_path)
        # Transform columns names and store into an sql database
        print('Transforming Dataframe: '+str(i))
        data = transform(data)
        print('Transformed: '+str(i))
        print('Loading Data into an SQLite Database')
        if i == 0:
            data.to_sql(table_name, con=con, index = False, if_exists='replace')
        else:
            data.to_sql(table_name, con=con, index = False, if_exists='append')
        print('Data Loaded into '+db_name+'\n')


if __name__ == '__main__':
    start = time.time()
    print('Fetching Data from '+config.URL)
    extract(config.URL)
    print('Done Fetching!')
    print(f'Duration {time.time() - start}')

    start = time.time()
    data = load(db_name=config.DB_NAME, file_urls=download_urls, table_name=config.TABLE_NAME)
    print(f'Duration {time.time() - start}')

