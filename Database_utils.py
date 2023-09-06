import yaml
import pandas as pd
import tabula
from sqlalchemy import create_engine, inspect


cloud_credentials = '/Users/paddy/Desktop/multinational_retail_centralisation/db_creds_aws.yaml'
local_credentials = '/Users/paddy/Desktop/multinational_retail_centralisation/db_creds_local.yaml'

class DatabaseConnector:
    def __init__(self, credentials) -> None:
        self.credentials = self.read_db_creds(credentials)
        self.engine = self.init_db_engine(self.credentials)
        self.list_db_tables()
        pass

    def read_db_creds(self, creds):
        with open(creds, 'r') as file:
            self.data = yaml.load(file, Loader=yaml.FullLoader)
        return self.data

    def init_db_engine(self, creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['AWS_HOST']
        PASSWORD = creds['AWS_PASSWORD']
        USER = creds['AWS_USER']
        DATABASE = creds['AWS_DATABASE']
        PORT = creds['AWS_PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return self.engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        self.table_list = inspector.get_table_names()
        print(self.table_list)
        # for column in inspector.get_columns(self.table_list):
        #     print("Column: %s" % column['name'])
        return self.table_list

    def upload_to_db(self, cleaned_dataframe, table_name):
        con = create_engine('postgresql+psycopg2://username:password@host:5432/sales_data')
        cleaned_dataframe.to_sql(table_name, con=con, if_exists='replace')


   



if __name__ == '__main__':
    db = DatabaseConnector(credentials=cloud_credentials)
    # db.read_db_creds()
    # db.init_db_engine()
    # db.list_db_tables()
    