# %%
from sqlalchemy import create_engine, inspect
import yaml 
import pandas as pd
import tabula
import requests
import boto3
from Database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self) -> None:
        pass
    
    def clean_user_data(self, table):
        self.table = table
        self.table['address'] = self.table['address'].replace('\n', ' ')
        print(table.info())
        print(table)

        return table










if __name__ == '__main__':
    db = DatabaseConnector()
    de = DataExtractor()
    dc = DataCleaning()
    raw_table = de.read_rds_table(db, table_name='legacy_users')
    dc.clean_user_data(raw_table)
