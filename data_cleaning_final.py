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

# The `DataCleaning` class is used for data cleaning and manipulation, with attributes representing
# different tables.
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
        """
        The function `datetime_transform` converts specified columns in a pandas DataFrame to datetime
        format.
        
        :param columns: A list of column names in the DataFrame that contain date or time values
        :type columns: list
        :param df: The `df` parameter is a pandas DataFrame that contains the data you want to transform
        :type df: pd.DataFrame
        :return: the transformed DataFrame with the specified date columns converted to datetime format.
        """
        date_cols = columns
        for date_col in date_cols:
            df.loc[:,date_col] = df.loc[:,date_col].apply(pd.to_datetime, 
                                            infer_datetime_format=True, 
                                            errors='coerce')
        return df

    @staticmethod
    def month_year_transform(columns: list, df: pd.DataFrame):
        """
        The function `month_year_transform` converts columns in a DataFrame to datetime format using the
        specified format.
        
        :param columns: A list of column names in the DataFrame that contain date values in the format
        'mm/yy'
        :type columns: list
        :param df: The `df` parameter is a pandas DataFrame that contains the data you want to
        transform. It should have columns that represent dates in the format 'mm/yy'
        :type df: pd.DataFrame
        :return: the transformed DataFrame with the specified date columns converted to datetime format.
        """
        date_cols = columns
        for date_col in date_cols:
            df.loc[:,date_col] = df.loc[:,date_col].apply(pd.to_datetime, 
                                            format='%m/%y', 
                                            errors='coerce')
        return df

    @staticmethod
    def column_value_set(column: str, df: pd.DataFrame):
        """
        The function `column_value_set` takes a column name and a DataFrame as input, and prints the
        unique values in that column.
        
        :param column: The column parameter is a string that represents the name of the column in the
        DataFrame that you want to get the unique values from
        :type column: str
        :param df: The parameter `df` is a pandas DataFrame, which is a two-dimensional labeled data
        structure with columns of potentially different types.
        :type df: pd.DataFrame
        """
        temp_list = df[column].tolist()
        print(set(temp_list))

    @staticmethod
    def kg_cov(value: str):
        """
        The function `kg_cov` removes the "kg" suffix from a given string if it exists.
        
        :param value: The parameter "value" is a string that represents a weight measurement
        :type value: str
        :return: The function `kg_cov` returns the input value without the last two characters if the
        last two characters are 'kg'. Otherwise, it returns the input value as is.
        """
        if value[-2:] =='kg':
            return value[:-2]
        else:
            return value

    @staticmethod
    def grams_and_ml(value: str):
        """
        The function "grams_and_ml" converts a value from grams or milliliters to kilograms or liters
        respectively.
        
        :param value: The parameter `value` is a string that represents a measurement in grams (g) or
        milliliters (ml)
        :type value: str
        :return: the value after converting it to grams.
        """
        if value[-1] == 'g' and value[-2].isdigit() and value[:-2].isdigit() or value[-2:] == 'ml':
            value = value.replace('g','').replace('ml','')
            value = int(value) /1000
        return value

    @staticmethod
    def multiply_values(value: str):
        """
        The function takes a string value, checks if it contains 'x', replaces ' x ' with a space,
        splits the string into two numbers, multiplies them, and returns the result divided by 1000.
        
        :param value: The parameter `value` is a string that represents a multiplication operation. It
        can have the format "num1 x num2", where `num1` and `num2` are numbers
        :type value: str
        :return: the multiplied value if the input string contains 'x', otherwise it returns the input
        value itself.
        """
        if 'x' in value:
            value = value.replace(' x ',' ')
            num1, num2 = value.split(' ')[0], value.split(' ')[1][:-1]
            new_value = (int(num1) * int(num2)) / 1000
            return new_value
        else:
            return value
    
    @staticmethod
    def oz_conversion(value: str):
        """
        The function `oz_conversion` converts a value from ounces to grams.
        
        :param value: The parameter "value" is a string that represents a measurement in ounces
        :type value: str
        :return: The value after converting it from ounces to grams.
        """
        if 'oz' in value:
            value = value.replace('oz', '')
            value = float(value) * 28.3495
        return value

    def clean_user_data(self):
        """
        The function `clean_user_data` cleans and transforms user data by removing duplicates, replacing
        characters in phone numbers, removing non-numeric characters, replacing country codes, filtering
        by specific country codes, filtering by user UUID length, transforming date columns, and
        dropping null values.
        :return: the cleaned users_table.
        """
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
        """
        The function `clean_card_data` cleans and transforms a table of card data by removing
        duplicates, null values, and non-numeric characters, and converting date columns to the correct
        datetime format.
        :return: the cleaned cards data.
        """
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
        """
        The function "clean_store_data" cleans and transforms a dataframe containing store data by
        removing unnecessary columns, removing duplicates, filtering by country code, replacing newline
        characters in addresses, removing letters from staff numbers, converting datetime values, and
        correcting misspelled continents.
        :return: the cleaned "stores" dataframe.
        """
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
        """
        The function `convert_product_weights` cleans and converts the weights of products in a product
        table.
        :return: the cleaned products table.
        """
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
        """
        The function `clean_orders_table` removes specific columns from the `orders_table` and returns
        the cleaned table.
        :return: the cleaned orders table.
        """
        orders = self.orders_table
        orders = orders.drop(columns=['first_name','last_name','1','level_0','index']).reindex()
        print('Orders table cleaned')
        return orders

    def clean_datetime_table(self):
        """
        The function `clean_datetime_table` cleans a datetime table by removing rows where the 'month'
        column is not a digit, dropping any rows with missing values, and removing duplicate rows.
        :return: the cleaned datetime table.
        """
        datetime_table = self.datetimes_table
        datetime_table = datetime_table[datetime_table.loc[:, 'month'].astype('str').apply(lambda x : x.isdigit())]
        datetime_table = datetime_table.dropna()
        datetime_table = datetime_table.drop_duplicates()
        print('Datetime table cleaned')
        return datetime_table