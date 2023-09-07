# %%
import yaml 
import pandas as pd
import tabula
import requests
import boto3
from Database_utils import DatabaseConnector
from data_extraction import DataExtractor
from decouple import config
from itertools import count
from sqlalchemy import create_engine, inspect

CLOUD_CREDS = config('CLOUD_YAML')
LOCAL_CREDS = config('LOCAL_YAML')

class DataCleaning:
    def __init__(self) -> None:
        pass
    
    def clean_user_data(self, table: pd.DataFrame):
        users_table = table
        print(len(users_table))
        users_table.drop_duplicates()
        # print(users_table['country_code'].unique())
        replace_dict = {
            '(': '',
            ')': '',
            '.': '',
            '-': '',
            ' ': '',
            '+': ''
            }
        users_table.loc[:, 'address'] = users_table['address'].apply(lambda x : x.replace('\n', ' ')) #remove \n in address
        users_table.loc[:, 'phone_number'] = users_table['phone_number'].apply(lambda x : x.translate(str.maketrans(replace_dict))) #replaces non numbers with empty
        users_table.loc[:, 'phone_number'] = users_table['phone_number'].str.replace(r'[a-zA-Z%]', '') # remove chars
        users_table.loc[:, 'country_code'] = users_table['country_code'].apply(lambda x : x.replace('GGB', 'GB'))
        users_table.loc[:, 'country_code'] = users_table.loc[users_table['country_code'].isin(['GB', 'US', 'DE'])]
        
        date_cols = ['date_of_birth','join_date']
        users_table = hf.datetime_transform(date_cols, users_table)
        # for date_col in date_cols:
        #     users_table.loc[:,date_col] = users_table.loc[:,date_col].apply(pd.to_datetime, 
        #                                     infer_datetime_format=True, 
        #                                     errors='coerce')
        users_table.drop_duplicates()
        users_table.dropna()
        print(table.info())
        print(table.head(20))

        return users_table





class CleaningHelperFunctions:
    def __init__(self) -> None:
        pass

    def datetime_transform(self, columns: list, df: pd.DataFrame):
        date_cols = columns
        for date_col in date_cols:
            df.loc[:,date_col] = df.loc[:,date_col].apply(pd.to_datetime, 
                                            infer_datetime_format=True, 
                                            errors='coerce')
        return df





if __name__ == '__main__':
    dc = DataCleaning()
    db = DatabaseConnector(creds=CLOUD_CREDS)
    # formatted_creds = db.read_db_creds(creds=CLOUD_CREDS)
    # engine = db.init_db_engine(formatted_creds)
    de = DataExtractor()
    hf = CleaningHelperFunctions()
    raw_table = de.read_rds_table(engine=db.engine, table_name='legacy_users')
    # print(raw_table)
    cleaned_res = dc.clean_user_data(raw_table)
    # local_creds = db.read_db_creds(creds=LOCAL_CREDS)
    db.upload_to_db(cleaned_dataframe=cleaned_res, table_name='dim_users', creds=LOCAL_CREDS)