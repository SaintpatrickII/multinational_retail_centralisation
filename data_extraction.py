# %%
import json
import tabula
import requests
import boto3
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
from io import StringIO
from database_utils import DatabaseConnector
from sqlalchemy import inspect
from decouple import config

CLOUD_CREDS = config('CLOUD_YAML')
STORE_API = config('STORE_API')
AWS_STORES = config('AWS_STORES')
AWS_ALL_STORES = config('AWS_ALL_STORES')
BUCKET_NAME = config('BUCKET_NAME')
S3_FILE = config('FILE_NAME')
JSON_FILE = config('JSON_FILE')
PDF_FILE = config('PDF_FILE')

# The class DataExtractor is a placeholder with an empty constructor.
class DataExtractor:
    def __init__(self) -> None:
        pass

    def list_db_tables(self, engine):
        """
        The function `list_db_tables` takes an engine object as input and returns a list of table names
        in the database connected to that engine.
        
        :param engine: The "engine" parameter is an instance of a database engine, such as SQLAlchemy's
        `create_engine` object. It is used to connect to the database and perform operations on it
        :return: a list of table names in the database.
        """
        inspector = inspect(engine)
        self.table_list = inspector.get_table_names()
        print(self.table_list)
        # print(self.table_list)
        # for column in inspector.get_columns(self.table_list):
        #     print("Column: %s" % column['name'])
        return self.table_list

    def read_rds_table(self, engine, table_name: pd.DataFrame):
        """
        The function reads a table from a database using the provided engine and table name, and returns
        the table as a pandas DataFrame.
        
        :param engine: The `engine` parameter is the database engine object that is used to establish a
        connection to the database. It is typically created using a library like SQLAlchemy
        :param table_name: The `table_name` parameter is the name of the table in the database that you
        want to read
        :type table_name: pd.DataFrame
        :return: a pandas DataFrame object named "pd_users" if the table name exists in the database
        tables. If the table name does not exist, it prints "Invalid Table" and does not return
        anything.
        """
        con = engine
        db_tables = self.list_db_tables(engine=engine)
        if table_name in db_tables:
            pd_users = pd.read_sql_table(table_name, con=con)
            return pd_users
        else:
            print('Invalid Table')
        
    def retrieve_pdf_data(self, filepath: str):
        """
        The function retrieves data from a PDF file and returns it as a pandas DataFrame.
        
        :param filepath: The filepath parameter is a string that represents the path to the PDF file
        that you want to retrieve data from
        :type filepath: str
        :return: a pandas DataFrame object named cc_df.
        """
        cc_df = tabula.read_pdf(filepath, stream=False, pages='all')
        print(cc_df[0])
        cc_df = pd.concat(cc_df)
        print(cc_df.head())
        return cc_df

    def list_number_of_stores(self, endpoint: str, header: dict):
        """
        The function retrieves the number of stores from an API endpoint using the provided header.
        
        :param endpoint: The `endpoint` parameter is a string that represents the API endpoint URL where
        you can retrieve the list of stores
        :type endpoint: str
        :param header: The `header` parameter is a dictionary that contains the API key required to
        access the endpoint
        :type header: dict
        :return: The number of stores in the stores_list.
        """
        api_endpoint = endpoint
        api_header = header
        true_header = {'x-api-key': api_header}
        response = requests.get(api_endpoint, headers=true_header).content
        stores_list = json.loads(response)
        return stores_list['number_stores']

    def retrieve_stores_data(self, endpoint: str, header: dict):
        """
        The function retrieves store data from an API endpoint, stores it in a dataframe, and returns
        the dataframe.
        
        :param endpoint: The `endpoint` parameter is a string that represents the API endpoint URL from
        where you want to retrieve the stores data. It should be in the format of a URL
        :type endpoint: str
        :param header: The `header` parameter is a dictionary that contains the headers to be included
        in the API request. In this case, it seems to include a single header with the key
        `'x-api-key'`. The value of this header is not specified in the code snippet, so you would need
        to provide the
        :type header: dict
        :return: a pandas DataFrame containing the data retrieved from the stores API.
        """
        curr_stores = []
        no_of_stores = self.list_number_of_stores(endpoint=AWS_ALL_STORES, header=STORE_API)
        api_endpoint = endpoint
        api_header = header
        true_header = {'x-api-key': api_header}
        for store in range(0, no_of_stores):
            response = requests.get(f'{api_endpoint}{store}', headers=true_header).json()
            curr_stores.append(pd.DataFrame(response,index=[np.NaN]))
        curr_stores_df = pd.concat(curr_stores)
        print(f'stores loaded into dataframe with {len(curr_stores_df)} rows :)')
        return curr_stores_df

    def extract_from_s3(self, bucket: str, file_from_s3: str):
        """
        The function extracts data from a file stored in an S3 bucket and returns it as a pandas
        DataFrame.
        
        :param bucket: The "bucket" parameter is the name of the S3 bucket from which you want to
        extract the file. It is a string that represents the name of the bucket
        :type bucket: str
        :param file_from_s3: The `file_from_s3` parameter is the name or key of the file that you want
        to extract from the S3 bucket. It is the file that you want to download and read as a pandas
        DataFrame
        :type file_from_s3: str
        :return: a pandas DataFrame object.
        """
        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=bucket, Key=file_from_s3)
        s3_data = s3_object['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(s3_data))
        print('S3 file Downloaded')
        return df

    def extract_from_s3_json(self, bucket: str, file_from_s3: str):
        """
        The function extracts data from a JSON file stored in an S3 bucket and returns it as a pandas
        DataFrame.
        
        :param bucket: The "bucket" parameter refers to the name of the S3 bucket where the JSON file is
        stored. An S3 bucket is a container for storing objects in Amazon S3
        :type bucket: str
        :param file_from_s3: The `file_from_s3` parameter is the name or key of the file that you want
        to download from the S3 bucket. It should be a string that specifies the file path or name in
        the S3 bucket
        :type file_from_s3: str
        :return: a pandas DataFrame object.
        """
        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=bucket, Key=file_from_s3)
        s3_data = s3_object['Body'].read().decode('utf-8')
        df = pd.read_json(StringIO(s3_data))
        print(df.head())
        print('S3 file Downloaded')
        return df


if __name__ == '__main__':
    db = DatabaseConnector(creds=CLOUD_CREDS)
    de = DataExtractor()
    table_list = de.list_db_tables(engine=db.engine)
    print(table_list)
    de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')




