import yaml
import pandas as pd
import tabula
from sqlalchemy import create_engine, inspect
from decouple import config

CLOUD_CREDS = config('CLOUD_YAML')

class DatabaseConnector:
    def __init__(self) -> None:
        # self.credentials = self.read_db_creds(credentials)
        # self.engine = self.init_db_engine(self.credentials)
        # self.list_db_tables()
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
        print('Database connected')
        return self.engine


    def upload_to_db(self, cleaned_dataframe: pd.DataFrame, table_name: str, creds):
        creds_formatted = self.read_db_creds(creds)
        connection = self.init_db_engine(creds_formatted)
        cleaned_dataframe.to_sql(table_name, con=connection, if_exists='replace')


   



if __name__ == '__main__':
    db = DatabaseConnector()
    formatted_creds = db.read_db_creds(creds=CLOUD_CREDS)
    db.init_db_engine(formatted_creds)

    # db.upload_to_db()
    