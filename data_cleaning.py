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
        users_table.drop_duplicates()
        users_table.dropna()
        print(table.info())
        print(table.head(20))

        return users_table

    
    def clean_card_data(self, card_data: pd.DataFrame):
        cards = card_data
        cards = card_data
        # print(cards.info())

        # cards table has no index, lets fix that
        index = [row for row in range(0, len(cards))] 
        cards['index'] = index                         # new column
        cards = cards.set_index(['index'])
        cards = cards.drop_duplicates()
        #  remove null
        cards = cards[cards.card_number != 'NULL']
        cards = cards.dropna()
        # enfore datatypes
        cards['card_number'] = cards['card_number'].astype('str')     
        cards['card_provider'] = cards['card_provider'].astype('str')     
        # remove rouge character
        cards.loc[:,'card_number'] = cards.loc[:,'card_number'].astype('str').apply(lambda x : x.replace('?', ''))
        # force numeric values
        cards = cards[cards['card_number'].str.isnumeric()] 
        # format dates
        cards.loc[:,'expiry_date'] = \
        cards.loc[:,'expiry_date'].apply(pd.to_datetime, format='%m/%y', errors='coerce')
        # cards = cards[cards.card_number != 'NaT']
        # print(cards[cards.index.duplicated()])
        cards.loc[:, 'date_payment_confirmed'] = \
        cards.loc[:,'date_payment_confirmed'].apply(pd.to_datetime,
                                                                infer_datetime_format=True,
                                                                errors='coerce')
        # drop duplicates & NAN                                                        # 
        # cards = cards.drop_duplicates()
        # cards = cards.dropna()
        print(cards.head(10))
        print('Card Cleaning Done!')

        return cards




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
    de = DataExtractor()
    hf = CleaningHelperFunctions()
    # raw_table = de.read_rds_table(engine=db.engine, table_name='legacy_users')
    # cleaned_res = dc.clean_user_data(raw_table)
    # db.upload_to_db(cleaned_dataframe=cleaned_res, table_name='dim_users', creds=LOCAL_CREDS)
    card_raw = de.retrieve_pdf_data(filepath='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    cleaned_cards = dc.clean_card_data(card_data=card_raw)
    db.upload_to_db(cleaned_dataframe=cleaned_cards, table_name='dim_card_details', creds=LOCAL_CREDS)