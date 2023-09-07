import os
import yaml 
import tabula
import requests
import boto3
import pandas as pd
from Database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect
from decouple import config

CLOUD_CREDS = config('CLOUD_YAML')


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
            print(pd_users)
            return pd_users
        else:
            print('Invalid Table')
        



        


if __name__ == '__main__':
    db = DatabaseConnector(creds=CLOUD_CREDS)
    # formatted_creds = db.read_db_creds(creds=CLOUD_CREDS)
    # engine = db.init_db_engine(formatted_creds)
    de = DataExtractor()
    de.list_db_tables(engine=db.engine)
    de.read_rds_table(engine=db.engine, table_name='legacy_users')
    # de.read_rds_table(table_name='legacy_users')
    # engine=db, table_name='legacy_users'
