from sqlalchemy import create_engine, inspect
import yaml 
import pandas as pd
import tabula
import requests
import boto3
from Database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, connector, table_name):
        """
        The function reads a table from a relational database and returns the data as a pandas DataFrame
        if the table exists, otherwise it prints an error message.
        
        :param connector: The `connector` parameter is an object that is responsible for connecting to
        the database and performing database operations. It should have methods like `read_db_creds()`
        to read the database credentials, `init_db_engine()` to initialize the database engine, and
        `list_db_tables()` to list all the tables
        :param table_name: The `table_name` parameter is the name of the table in the database that you
        want to read
        :return: the data from the specified table in the form of a pandas DataFrame.
        """
        connector.read_db_creds()
        engine = connector.init_db_engine()
        db_tables = connector.list_db_tables()
        if table_name in db_tables:
            users = pd.read_sql_table(table_name, engine)
            print((users))
            return users
        else:
            print('Invalid Table')
        



        


if __name__ == '__main__':
    db = DatabaseConnector()
    de = DataExtractor()
    de.read_rds_table(db, table_name='legacy_users')
