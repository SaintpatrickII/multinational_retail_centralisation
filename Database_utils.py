import yaml
import pandas as pd
import tabula
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            self.data = yaml.load(file, Loader=yaml.FullLoader)

    def init_db_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.data['RDS_HOST']
        PASSWORD = self.data['RDS_PASSWORD']
        USER = self.data['RDS_USER']
        DATABASE = self.data['RDS_DATABASE']
        PORT = self.data['RDS_PORT']
