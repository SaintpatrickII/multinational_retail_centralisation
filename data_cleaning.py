# %%
from ast import Num, Return
from cmath import nan
import csv
from curses.ascii import isalnum
from math import prod
import re
from tkinter.ttk import Separator
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
STORE_API = config('STORE_API')
AWS_STORES = config('AWS_STORES')
AWS_ALL_STORES = config('AWS_ALL_STORES')
BUCKET_NAME = config('BUCKET_NAME')
S3_FILE = config('FILE_NAME')

class DataCleaning:
    def __init__(self) -> None:
        pass
    
    def clean_user_data(self, user_data: pd.DataFrame):
        users_table = user_data
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
        print(users_table.info())
        print(users_table.head(20))

        return users_table

    
    def clean_card_data(self, card_data: pd.DataFrame):
        cards = card_data
        cards = card_data
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
        # convert dates to correct datetime variables
        month_year_cols = ['expiry_date']
        cards = hf.month_year_transform(month_year_cols, cards)
        date_cols = ['date_payment_confirmed']
        cards = hf.datetime_transform(date_cols, cards)
        print(cards.head(10))
        print('Card Cleaning Done!')

        return cards

    def clean_store_data(self, store_data: pd.DataFrame):
        print('Cleaning Store Dataframe')
        stores = store_data
        # stores = stores.iloc[:, 1:] #remove dummy index
        index = [row for row in range(0, len(stores))] 
        stores['index'] = index                         # new column
        stores = stores.set_index(['index'])
        stores = stores.drop_duplicates()
        # get rid of random country_codes
        stores.loc[:, 'country_code'] = stores.loc[stores['country_code'].isin(['GB', 'US', 'DE'])]
        stores.loc[:, 'address'] = stores['address'].str.replace('\n', ' ') #remove \n in address
        stores.loc[:,'staff_numbers'] = stores['staff_numbers'].astype('str').apply(lambda x : re.sub('\D','',x)) #remove letters from staff_numbers
        # convert datetime
        datetime_list = ['opening_date']
        stores = hf.datetime_transform(datetime_list, stores)
        # hf.column_value_set('continent', stores)
        stores[['continent']] = stores[['continent']] \
                                    .apply(lambda x:x.replace('eeEurope','Europe')) \
                                    .apply(lambda x:x.replace('eeAmerica','America'))
        hf.column_value_set('continent', stores)
        print(stores.head())
        print(len(stores))
        print('Store Dataframe Cleaned')
        return stores


    def convert_product_weights(self, product_data: pd.DataFrame):
        products = product_data
        products.loc[:, 'product_price'] = products['product_price'].astype('str').apply(lambda x : x.replace('£', ''))

        def correct_format(value):
            if value[-1].isalnum() is False:
                wrong_char = value[-1]
                value= value.replace(wrong_char,'').strip()
            return value
        products.loc[:,'weight'] =products.loc[:,'weight'].astype('str').apply(lambda x:correct_format(x))

        def barcode_weights(value: str):
            if value[0].isalpha() is True or value[1].isalpha() is True:
                new_value = 'NaN'
                return new_value
            else:
                return value
        
        def grams_and_ml(value: str):
            if value[-1] == 'g' and value[-2].isdigit() and value[:-2].isdigit() or value[-2:] == 'ml':
                value = value.replace('g','').replace('ml','')
                value = int(value) /1000
                return value
            elif '.' in value and value[-2].isdigit():
                value = value.replace('.',' ')
                num1, num2 = value.split(' ')[0], value.split(' ')[1][:-1]
                str_to_join = ['0.', num1, num2]
                kg_val = ''.join(str_to_join)
                return kg_val
            elif 'kg' in value:
                return value[:-2]
            else:
                return value

        def multiply_values(value):
            if 'x' in value:
                value = value.replace(' x ',' ')
                num1, num2 = value.split(' ')[0], value.split(' ')[1][:-1]
                new_value = (int(num1) * int(num2)) / 1000
                return new_value
            else:
                return value

        def oz_conversion(value):
            if 'oz' in value:
                value = value.replace('oz', '')
                value = float(value) * 28.3495
            return value

        products.loc[:, 'weight'] = products.loc[:, 'weight'].astype('str').apply(lambda x : barcode_weights(x))
        products.loc[:,'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x:grams_and_ml(x))
        products.loc[:, 'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x : multiply_values(x))
        products.loc[:,'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x:oz_conversion(x))
        products.dropna()
        products.drop_duplicates()
        products = products[products.weight != 'NaN']
        print('products claned :)')
        return products

        


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

    def month_year_transform(self, columns: list, df: pd.DataFrame):
        date_cols = columns
        for date_col in date_cols:
            df.loc[:,date_col] = df.loc[:,date_col].apply(pd.to_datetime, 
                                            format='%m/%y', 
                                            errors='coerce')
        return df

    def column_value_set(self, column: str, df):
        temp_list = df[column].tolist()
        print(set(temp_list))


if __name__ == '__main__':
    dc = DataCleaning()
    db = DatabaseConnector(creds=CLOUD_CREDS)
    de = DataExtractor()
    hf = CleaningHelperFunctions()

    # users cleaning
    # users_raw = de.read_rds_table(engine=db.engine, table_name='legacy_users')
    # cleaned_res = dc.clean_user_data(users_raw)
    # db.upload_to_db(cleaned_dataframe=cleaned_res, table_name='dim_users', creds=LOCAL_CREDS)

    # cards cleaning
    # card_raw = de.retrieve_pdf_data(filepath='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # cleaned_cards = dc.clean_card_data(card_data=card_raw)
    # db.upload_to_db(cleaned_dataframe=cleaned_cards, table_name='dim_card_details', creds=LOCAL_CREDS)

    # stores cleaning
    # stores_raw = de.retrieve_stores_data(endpoint=AWS_STORES, header=STORE_API)
    # cleaned_stores = dc.clean_store_data(store_data=stores_raw)
    # db.upload_to_db(cleaned_dataframe=cleaned_stores, table_name='dim_stores_details', creds=LOCAL_CREDS)

    # products cleaning
    products_raw = de.extract_from_s3(bucket=BUCKET_NAME, file_from_s3=S3_FILE)
    cleaned_products = dc.convert_product_weights(product_data=products_raw)
    db.upload_to_db(cleaned_dataframe=cleaned_products, table_name='dim_product_details', creds=LOCAL_CREDS)