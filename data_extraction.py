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
        connector.read_db_creds()
        engine = connector.init_db_engine()
        db_tables = connector.list_db_tables()
        if table_name in db_tables:
            users = pd.read_sql_table(table_name, engine)
            # print((users))
            return users
        else:
            print('Invalid Table')
        



        


if __name__ == '__main__':
    db = DatabaseConnector()
    de = DataExtractor()
    de.read_rds_table(db, table_name='legacy_users')
