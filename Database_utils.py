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
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return self.engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        self.table_list = inspector.get_table_names()
        print(self.table_list)
        # for column in inspector.get_columns(self.table_list):
        #     print("Column: %s" % column['name'])
        return self.table_list

   



if __name__ == '__main__':
    db = DatabaseConnector()
    db.read_db_creds()
    db.init_db_engine()
    db.list_db_tables()