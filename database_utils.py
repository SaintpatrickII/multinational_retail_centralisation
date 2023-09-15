import yaml
import pandas as pd
import tabula
from sqlalchemy import create_engine, inspect
from decouple import config

CLOUD_CREDS = config('CLOUD_YAML')
LOCAL_CREDS = config('LOCAL_YAML')

class DatabaseConnector:
    def __init__(self, creds) -> None:
        self.credentials = self.read_db_creds(creds)
        self.engine = self.init_db_engine(self.credentials)
        # self.list_db_tables()
        pass

    def read_db_creds(self, creds):
        with open(creds, 'r') as file:
            self.data = yaml.load(file, Loader=yaml.FullLoader)
        return self.data

    def init_db_engine(self, creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['HOST']
        PASSWORD = creds['PASSWORD']
        USER = creds['USER']
        DATABASE = creds['DATABASE']
        PORT = creds['PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        print('Database connected')
        return self.engine


    def upload_to_db(self, cleaned_dataframe: pd.DataFrame, table_name: str, creds: str):
        creds_formatted = self.read_db_creds(creds)
        connection = self.init_db_engine(creds_formatted)
        print('almost there')
        cleaned_dataframe.to_sql(table_name, con=connection, if_exists='replace')
        print('cleaned dataframe uploaded to postgreSQL')


if __name__ == '__main__':
    db = DatabaseConnector(creds=CLOUD_CREDS)
    # formatted_creds = db.read_db_creds(creds=CLOUD_CREDS)
    # db.init_db_engine(formatted_creds)

    # db.upload_to_db()
    