from decouple import config
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning_final import DataCleaning

CLOUD_CREDS = config('CLOUD_YAML')
LOCAL_CREDS = config('LOCAL_YAML')
STORE_API = config('STORE_API')
AWS_STORES = config('AWS_STORES')
AWS_ALL_STORES = config('AWS_ALL_STORES')
BUCKET_NAME = config('BUCKET_NAME')
S3_FILE = config('FILE_NAME')
JSON_FILE = config('JSON_FILE')
PDF_FILE = config('PDF_FILE')

if __name__ == '__main__':
    def orders_run():
        db2 = DatabaseConnector(creds=CLOUD_CREDS)
        de2 = DataExtractor()
        table_list = de2.list_db_tables(engine=db2.engine)
        orders_raw = de2.read_rds_table(engine=db2.engine, table_name=table_list[2])
        orders_cleaned_init = DataCleaning(orders_table=orders_raw)
        cleaned_orders = orders_cleaned_init.clean_orders_table()
        db2.upload_to_db(cleaned_dataframe=cleaned_orders, table_name='orders_table', creds=LOCAL_CREDS)
    orders_run()

    db = DatabaseConnector(creds=CLOUD_CREDS)
    de = DataExtractor()
   
    def users_run():
        table_list = de.list_db_tables(engine=db.engine)
        users_raw = de.read_rds_table(engine=db.engine, table_name=table_list[1])
        user_clean_init = DataCleaning(users_table=users_raw)
        cleaned_res = user_clean_init.clean_user_data()
        db.upload_to_db(cleaned_dataframe=cleaned_res, table_name='dim_users', creds=LOCAL_CREDS)
    users_run()

    def cards_run():
        card_raw = de.retrieve_pdf_data(filepath=PDF_FILE)
        card_cleaned_init = DataCleaning(cards_table=card_raw)
        cleaned_cards = card_cleaned_init.clean_card_data()
        db.upload_to_db(cleaned_dataframe=cleaned_cards, table_name='dim_card_details', creds=LOCAL_CREDS)
    cards_run()

    def stores_run():
        stores_raw = de.retrieve_stores_data(endpoint=AWS_STORES, header=STORE_API)
        stores_clean_init = DataCleaning(stores_table=stores_raw)
        cleaned_stores = stores_clean_init.clean_store_data()
        db.upload_to_db(cleaned_dataframe=cleaned_stores, table_name='dim_store_details', creds=LOCAL_CREDS)
    stores_run()

    def products_run():
        products_raw = de.extract_from_s3(bucket=BUCKET_NAME, file_from_s3=S3_FILE)
        product_clean_init = DataCleaning(products_table=products_raw)
        cleaned_products = product_clean_init.convert_product_weights()
        db.upload_to_db(cleaned_dataframe=cleaned_products, table_name='dim_products', creds=LOCAL_CREDS)
    products_run()

    def datetime_run():
        datetime_raw = de.extract_from_s3_json(bucket=BUCKET_NAME, file_from_s3=JSON_FILE)
        datetime_clean_init = DataCleaning(datetimes_table=datetime_raw)
        cleaned_datetime = datetime_clean_init.clean_datetime_table()
        db.upload_to_db(cleaned_dataframe=cleaned_datetime, table_name='dim_date_times', creds=LOCAL_CREDS)
    datetime_run()
    print('All Cleaning Done')

