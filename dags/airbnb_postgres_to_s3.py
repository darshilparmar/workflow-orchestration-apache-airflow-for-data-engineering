from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from datetime import datetime, timedelta
import pandas as pd
import requests
import csv
import psycopg2
import numpy as np 
from custom_operator.postgres_to_s3_operator import PostgresToS3Operator


#Define Args
default_args = {
    "owner": "darshil",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 18),
}

#Define the DAG
dag = DAG(
    dag_id="airbnb_postgres_to_s3",
    default_args=default_args,
    description="Download, preprocess and load data form Postgres to S3",
    schedule_interval=timedelta(days=1),
)

listing_dates = ["2024-03-11"]

def download_csv():
    listing_url_template= "https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{date}/visualisations/listings.csv"
    for date in listing_dates:
        url = listing_url_template.format(date=date)
        response = requests.get(url)
        if response.status_code == 200:
            with open(f"/tmp/airbnbdata/listing-{date}.csv", 'wb') as f:
                f.write(response.content)
        else:
            print(f"failed to download {url}")

download_csv_task = PythonOperator(
    task_id="download_csv",
    python_callable=download_csv,
    dag=dag,
)
 
def preprocess_csv():
    for date in listing_dates:
        input_file = f'/tmp/airbnbdata/listing-{date}.csv'
        output_file = f'/tmp/airbnbdata/listing-{date}-processed.csv'
        df = pd.read_csv(input_file)
        df.fillna('', inplace=True)
        df.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)

preprocess_csv_task = PythonOperator(
    task_id="preprocess_csv",
    python_callable=preprocess_csv,
    dag=dag,
)

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="airbnb_postgres",
    sql="""
    DROP TABLE listings;
    CREATE TABLE IF NOT EXISTS listings (
        id BIGINT,
        name TEXT,
        host_id INTEGER,
        host_name VARCHAR(100),
        neighbourhood_group VARCHAR(100),
        neighbourhood VARCHAR(100),
        latitude NUMERIC(18,16),
        longitude NUMERIC(18,16),
        room_type VARCHAR(100),
        price VARCHAR(100),
        minimum_nights INTEGER,
        number_of_reviews INTEGER,
        last_review VARCHAR(100),
        reviews_per_month VARCHAR(100),
        calculated_host_listings_count INTEGER,
        availability_365 INTEGER,
        number_of_reviews_ltm INTEGER,
        license VARCHAR(100)
    );
    """,
    dag=dag,

)

def load_csv_to_postgres():
    conn = psycopg2.connect("dbname='postgres' user='airflow' host='postgres' password='airflow'")
    cur = conn.cursor()
    for date in listing_dates:
        processed_csv = f'/tmp/airbnbdata/listing-{date}-processed.csv'
        with open(processed_csv, 'r') as f:
            next(f)
            cur.copy_expert("COPY listings FROM stdin WITH CSV HEADER QUOTE '\"' ", f)
        conn.commit()
    cur.close()
    conn.close()

load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_to_postgres,
    dag=dag,
)

transfer_postgres_to_s3 = PostgresToS3Operator(
    task_id="transfer_postgres_to_s3",
    postgres_conn_id='airbnb_postgres',
    query='SELECT * FROM listings',
    s3_conn_id='aws_s3_airbnb',
    s3_bucket='darshil-test-bucket',
    s3_key='airbnb_test/postgres_data.csv',
    dag=dag,

)


download_csv_task >> preprocess_csv_task >> create_table >> load_csv_task >> transfer_postgres_to_s3