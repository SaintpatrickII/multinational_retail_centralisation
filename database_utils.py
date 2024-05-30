import yaml
import pandas as pd
from sqlalchemy import create_engine
from decouple import config

print('test')
CLOUD_CREDS = config('CLOUD_YAML')
LOCAL_CREDS = config('LOCAL_YAML')

# The `DatabaseConnector` class initializes a database connection using provided credentials.
class DatabaseConnector:
    def __init__(self, creds) -> None:
        self.credentials = self.read_db_creds(creds)
        self.engine = self.init_db_engine(self.credentials)
        pass

    def read_db_creds(self, creds):
        """
        The function reads database credentials from a file and returns the data.
        
        :param creds: The parameter "creds" is the file path to the credentials file that contains the
        database credentials
        :return: The `self.data` variable is being returned.
        """
        with open(creds, 'r') as file:
            self.data = yaml.load(files, Loader=yaml.FullLoader)
        return self.data

    def init_db_engine(self, creds):
        """
        The function initializes a database engine using the provided credentials and returns the engine
        object.
        
        :param creds: The `creds` parameter is a dictionary that contains the following keys:
        :return: the database engine object.
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['HOST']
        PASSWORD = creds['PASSWORD']
        USER = creds['USER']
        DATABASE = creds['DATABASE']
        PORT = creds['PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        print(self.engine)
        print('Database connected')
        return self.engine

    def upload_to_db(self, cleaned_dataframe: pd.DataFrame, table_name: str, creds: str):
        """
        The function uploads a cleaned dataframe to a PostgreSQL database table.
        
        :param cleaned_dataframe: A pandas DataFrame that has been cleaned and is ready to be uploaded
        to a database
        :type cleaned_dataframe: pd.DataFrame
        :param table_name: The `table_name` parameter is a string that specifies the name of the table
        in the database where you want to upload the cleaned dataframe
        :type table_name: str
        :param creds: The `creds` parameter is a string that represents the path or location of a file
        containing the credentials needed to connect to the database
        :type creds: str
        """
        creds_formatted = self.read_db_creds(creds)
        connection = self.init_db_engine(creds_formatted)
        print('almost there')
        cleaned_dataframe.to_sql(table_name, con=connection, if_exists='replace')
        print('cleaned dataframe uploaded to postgreSQL')


if __name__ == '__main__':
    db = DatabaseConnector(creds=CLOUD_CREDS)
    