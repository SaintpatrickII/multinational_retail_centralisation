# %%
import os
import re
import yaml 
import json
import tabula
import requests
import boto3
import requests
import pandas as pd
import numpy as np
from Database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect
from decouple import config

CLOUD_CREDS = config('CLOUD_YAML')
STORE_API = config('STORE_API')
AWS_STORES = config('AWS_STORES')
AWS_ALL_STORES = config('AWS_ALL_STORES')



class DataExtractor:
    def __init__(self) -> None:
        # self.engine = engine
        # self.list_db_tables(engine)
        # self.read_rds_table(table_name, engine=self.engine)
        pass

    def list_db_tables(self, engine):
        # engine = self.engine
        inspector = inspect(engine)
        self.table_list = inspector.get_table_names()
        # print(self.table_list)
        # for column in inspector.get_columns(self.table_list):
        #     print("Column: %s" % column['name'])
        return self.table_list


    def read_rds_table(self, engine, table_name: pd.DataFrame):
        con = engine
        # connector.read_db_creds()
        # engine = self.engine.init_db_engine(creds=CLOUD_CREDS)
        db_tables = self.list_db_tables(engine=engine)
        # print('*'*10)
        if table_name in db_tables:
            pd_users = pd.read_sql_table(table_name, con=con)
            # print(table_name)
            # print(pd_users)
            return pd_users
        else:
            print('Invalid Table')
        
    def retrieve_pdf_data(self, filepath: str):
        cc_df = tabula.read_pdf(filepath, stream=False, pages='all')
        cc_df = pd.concat(cc_df)
        # print(cc_df.head(100))
        return cc_df


    def list_number_of_stores(self, endpoint: str, header: dict):
        api_endpoint = endpoint
        api_header = header
        true_header = {'x-api-key': api_header}
        response = requests.get(api_endpoint, headers=true_header).content
        stores_list = json.loads(response)
        return stores_list['number_stores']

    def retrieve_stores_data(self, endpoint: str, header: dict):
        curr_stores = []
        no_of_stores = self.list_number_of_stores(endpoint=AWS_ALL_STORES, header=STORE_API)
        api_endpoint = endpoint
        api_header = header
        true_header = {'x-api-key': api_header}
        for store in range(0, no_of_stores):
            print(f'store number {store} proccessing')
            response = requests.get(f'{api_endpoint}{store}', headers=true_header).json()
            curr_stores.append(pd.DataFrame(response,index=[np.NaN]))
        curr_stores_df = pd.concat(curr_stores)
        print(f'stores loaded into dataframe with {len(curr_stores_df)} rows :)')
        return curr_stores_df



        


if __name__ == '__main__':
    db = DatabaseConnector(creds=CLOUD_CREDS)
    # formatted_creds = db.read_db_creds(creds=CLOUD_CREDS)
    # engine = db.init_db_engine(formatted_creds)
    de = DataExtractor()
    # de.list_db_tables(engine=db.engine)
    # de.read_rds_table(engine=db.engine, table_name='legacy_users')
    # de.retrieve_pdf_data(filepath='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # de.list_number_of_stores(endpoint=AWS_ALL_STORES, header=STORE_API)
    raw_stores = de.retrieve_stores_data(endpoint=AWS_STORES, header=STORE_API)
    # save_df_to_csv = raw_stores.to_csv('test.csv')
    # de.read_rds_table(table_name='legacy_users')
    # engine=db, table_name='legacy_users'
