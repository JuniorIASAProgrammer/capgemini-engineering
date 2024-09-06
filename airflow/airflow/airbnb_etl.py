from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from airflow.hooks.base import BaseHook
import os
import pandas as pd
import logging


RAW_DATA_PATH = '/opt/airflow/dags/data/raw/AB_NYC_2019.csv'
TRANSFORMED_DATA_PATH = '/opt/airflow/dags/data/transformed/AB_NYC_2019_transformed.csv'
FAILURE_LOG_FILE_PATH = '/opt/airflow/logs/airflow_failures.log'
QUALITY_LOG_FILE_PATH = './logs/data_quality_errors.log'
AIRFLOW_POSTGRES_CONNECTION_ID = 'airflow-airbnb'
POSTGRES_TABLE_NAME = 'airbnb_listings'

conn = BaseHook.get_connection(AIRFLOW_POSTGRES_CONNECTION_ID)
engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")


def failure_callback(context):
    task_instance = context['task_instance']
    with open(FAILURE_LOG_FILE_PATH, 'a') as log:
        log.write(f"Task {task_instance.task_id} failed at {datetime.now()}\n")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

@dag(
    dag_id='nyc_airbnb_etl',
    default_args=default_args,
    start_date=datetime(2024, 9, 5),
    catchup=False,
    schedule_interval='@daily'
)
def nyc_airbnb_etl_dag():  
    @task
    def ingest_data_task():
        """
        Check if file 
        """
        if not os.path.exists(RAW_DATA_PATH):
            raise FileNotFoundError(f"File {RAW_DATA_PATH} does not exist.")
        df = pd.read_csv(RAW_DATA_PATH)
        logging.info("------------Data ingestion-----------")
        logging.info(f"File path - {RAW_DATA_PATH}")
        logging.info("Data ingestion completed.")
        return df

    @task
    def transform_data_task():
        """
        Read all data and make all necessary transformations: filtering, cleaning, format converting, nan-value handling
        """
        data = pd.read_csv(RAW_DATA_PATH)
        # Filter out rows where price is 0 or negative
        data = data[data['price'] > 0]
        # Convert last_review to a datetime object
        data['last_review'] = pd.to_datetime(data['last_review'], errors='coerce')
        # Handle missing (if any) last_review dates by filling them with the earliest date in the dataset or a default date.
        earliest_date = data['last_review'].min()
        data['last_review'].fillna(earliest_date, inplace=True)
        # Handle missing values in reviews_per_month by filling them with 0.
        data['reviews_per_month'].fillna(0, inplace=True)
        # Drop any rows(if any) with missing latitude or longitude values
        data.dropna(axis='index', how='any', subset=['latitude', 'longitude'], inplace=True)
        data.to_csv(TRANSFORMED_DATA_PATH, index=False)
        logging.info("Data transformation completed.")


    # check if target table exists
    create_listing_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id=AIRFLOW_POSTGRES_CONNECTION_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                host_id INTEGER,
                host_name TEXT,
                neighbourhood_group TEXT,
                neighbourhood TEXT,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                room_type TEXT,
                price INTEGER,
                minimum_nights INTEGER,
                number_of_reviews INTEGER,
                last_review DATE,
                reviews_per_month DECIMAL(5,2),
                calculated_host_listings_count INTEGER,
                availability_365 INTEGER
        );""".format(POSTGRES_TABLE_NAME)
    )
    
    @task
    def load_data_to_postgres():
        """"
        Load processed data from .csv file to a postgres table
        """
        try:
            df = pd.read_csv(TRANSFORMED_DATA_PATH)
            df.to_sql(POSTGRES_TABLE_NAME, con=engine, if_exists='replace', index=False, method='multi')
        except SQLAlchemyError as sae:
            logging.info("SQLAlchemy error occurred while loading data.")
            logging.info(f"Error details: {sae}")
        except FileNotFoundError:
            logging.info(f"Error: The file at {TRANSFORMED_DATA_PATH} was not found.")
        except pd.errors.ParserError:
            logging.info("Error reading the CSV file. Please check the file format and contents.")
        except Exception as e:
            logging.info("An unexpected error occurred while loading data to PostgreSQL.")
            logging.info(f"Error details: {e}")

    def check_data_quality():
        try:
            df = pd.read_csv(TRANSFORMED_DATA_PATH)
            expected_count = len(df)
            actual_count = pd.read_sql(f"SELECT COUNT(*) FROM {POSTGRES_TABLE_NAME}", con=engine).iloc[0, 0]
            if actual_count != expected_count:
                logging.info(f"Data quality check failed: Expected {expected_count} rows, but found {actual_count} in the table.")
                return 'log_error'
            null_check_query = f"""
                SELECT COUNT(*) FROM {POSTGRES_TABLE_NAME}
                WHERE price IS NULL OR minimum_nights IS NULL OR availability_365 IS NULL;
            """
            null_count = pd.read_sql(null_check_query, con=engine).iloc[0, 0]
            if null_count > 0:
                logging.info(f"Data quality check failed: Found {null_count} NULL values in critical columns.")
                return 'log_error'
            logging.info("Data quality checks passed successfully.")
            return 'proceed'
        except SQLAlchemyError as e:
            logging.info(f"SQLAlchemy error during data quality checks: {e}")
            return 'log_error'
        
    quality_check_task = BranchPythonOperator(
        task_id='data_quality_checks',
        python_callable=check_data_quality,
        trigger_rule='one_success',
        provide_context=True
    )
    
    @task
    def proceed():
        logging.info("Test were passed")

    @task
    def log_error():
        with open(QUALITY_LOG_FILE_PATH, 'a') as log_file:
            log_file.write("Data quality checks failed. See above for details.\n")
        logging.info("Error logged and further processing stopped.")


    ingest_data_task() >> transform_data_task() >> create_listing_table >> load_data_to_postgres() >> quality_check_task
    quality_check_task >> log_error()
    quality_check_task >> proceed()


dag = nyc_airbnb_etl_dag()