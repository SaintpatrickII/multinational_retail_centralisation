import re
import pandas as pd
from decouple import config

CLOUD_CREDS = config('CLOUD_YAML')
LOCAL_CREDS = config('LOCAL_YAML')
STORE_API = config('STORE_API')
AWS_STORES = config('AWS_STORES')
AWS_ALL_STORES = config('AWS_ALL_STORES')
BUCKET_NAME = config('BUCKET_NAME')
S3_FILE = config('FILE_NAME')
JSON_FILE = config('JSON_FILE')
PDF_FILE = config('PDF_FILE')

class DataCleaning:
    def __init__(self, users_table=None,
                       cards_table=None,
                       stores_table=None,
                       products_table=None,
                       orders_table=None,
                       datetimes_table=None) -> None:
        self.users_table = users_table
        self.cards_table = cards_table
        self.stores_table = stores_table
        self.product_table = products_table
        self.orders_table = orders_table
        self.datetimes_table = datetimes_table

    @staticmethod
    def datetime_transform(columns: list, df: pd.DataFrame):
        date_cols = columns
        for date_col in date_cols:
            df.loc[:,date_col] = df.loc[:,date_col].apply(pd.to_datetime, 
                                            infer_datetime_format=True, 
                                            errors='coerce')
        return df

    @staticmethod
    def month_year_transform(columns: list, df: pd.DataFrame):
        date_cols = columns
        for date_col in date_cols:
            df.loc[:,date_col] = df.loc[:,date_col].apply(pd.to_datetime, 
                                            format='%m/%y', 
                                            errors='coerce')
        return df

    @staticmethod
    def column_value_set(column: str, df: pd.DataFrame):
        temp_list = df[column].tolist()
        print(set(temp_list))

    @staticmethod
    def kg_cov(value: str):
            if value[-2:] =='kg':
                return value[:-2]
            else:
                return value

    @staticmethod
    def grams_and_ml(value: str):
            if value[-1] == 'g' and value[-2].isdigit() and value[:-2].isdigit() or value[-2:] == 'ml':
                value = value.replace('g','').replace('ml','')
                value = int(value) /1000
            return value

    @staticmethod
    def multiply_values(value: str):
            if 'x' in value:
                value = value.replace(' x ',' ')
                num1, num2 = value.split(' ')[0], value.split(' ')[1][:-1]
                new_value = (int(num1) * int(num2)) / 1000
                return new_value
            else:
                return value
    
    @staticmethod
    def oz_conversion(value: str):
            if 'oz' in value:
                value = value.replace('oz', '')
                value = float(value) * 28.3495
            return value

    def clean_user_data(self):
        users_table = self.users_table
        users_table.drop_duplicates()
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
        users_table = users_table[users_table['user_uuid'].str.len()==36]
        date_cols = ['date_of_birth','join_date']
        users_table = self.datetime_transform(date_cols, users_table)
        users_table.drop_duplicates()
        users_table.dropna()
        print('User Data Cleaned')
        return users_table

    def clean_card_data(self):
        cards = self.cards_table
        index = [row for row in range(0, len(cards))] # cards table has no index, lets fix that
        cards['index'] = index                         # new column
        cards = cards.set_index(['index'])
        cards = cards.drop_duplicates()
        cards = cards[cards.card_number != 'NULL'] #  remove null
        cards = cards.dropna()
        cards['card_number'] = cards['card_number'].astype('str')  # enforce datatypes  
        cards['card_provider'] = cards['card_provider'].astype('str')     
        cards.loc[:,'card_number'] = cards.loc[:,'card_number'].astype('str').apply(lambda x : x.replace('?', '')) # remove rouge character
        cards = cards[cards['card_number'].str.isnumeric()] # force numeric values
        month_year_cols = ['expiry_date'] # convert dates to correct datetime variables
        cards = self.month_year_transform(month_year_cols, cards)
        date_cols = ['date_payment_confirmed']
        cards = self.datetime_transform(date_cols, cards)
        print('Card Cleaning Done!')
        return cards

    def clean_store_data(self):
        print('Cleaning Store Dataframe')
        stores = self.stores_table
        stores = stores.iloc[:, 1:] #remove dummy index
        index = [row for row in range(0, len(stores))] 
        stores['index'] = index                         # new column
        stores = stores.set_index(['index'])
        stores = stores.drop_duplicates()
        stores = stores[stores.country_code.isin(['GB', 'US', 'DE'])] # get rid of random country_codes
        stores.loc[:, 'address'] = stores['address'].str.replace('\n', ' ') #remove \n in address
        stores.loc[:,'staff_numbers'] = stores['staff_numbers'].astype('str').apply(lambda x : re.sub('\D','',x)) #remove letters from staff_numbers
        datetime_list = ['opening_date'] # convert datetime & mispelled continents
        stores = self.datetime_transform(datetime_list, stores)
        stores[['continent']] = stores[['continent']] \
                                    .apply(lambda x:x.replace('eeEurope','Europe')) \
                                    .apply(lambda x:x.replace('eeAmerica','America'))
        self.column_value_set('continent', stores)
        print('Store Dataframe Cleaned')
        return stores

    def convert_product_weights(self):
        products = self.product_table
        products.loc[:, 'product_price'] = products['product_price'].astype('str').apply(lambda x : x.replace('£', ''))
        products = products[~products['product_price'].str.contains("[a-zA-Z]").fillna(False)]
        products.loc[:, 'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x : self.kg_cov(x)) #-> kg conversion works
        products.loc[:,'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x: self.oz_conversion(x)) #-> oz works correctly
        products.loc[:,'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x: self.multiply_values(x)) #-> x conversion works
        products.loc[:,'weight'] = products.loc[:,'weight'].astype('str').apply(lambda x: self.grams_and_ml(x))
        products.loc[:,'weight'] = products[products.loc[:,'weight'].astype('str').apply(lambda x : x.replace('.','').isdigit())]
        products.loc[:,'weight'] = products.loc[:,'weight'].astype('float').apply(lambda x : round(x,2))
        datetime_col = ['date_added']
        products = self.datetime_transform(datetime_col, products)
        products = products[products.weight != 'NaN']
        print('Products table Cleaned')
        return products

    def clean_orders_table(self):
        orders = self.orders_table
        orders = orders.drop(columns=['first_name','last_name','1','level_0','index']).reindex()
        print('Orders table cleaned')
        return orders

    def clean_datetime_table(self):
        datetime_table = self.datetimes_table
        datetime_table = datetime_table[datetime_table.loc[:, 'month'].astype('str').apply(lambda x : x.isdigit())]
        datetime_table = datetime_table.dropna()
        datetime_table = datetime_table.drop_duplicates()
        print('Datetime table cleaned')
        return datetime_table