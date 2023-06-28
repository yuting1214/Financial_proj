from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from scrape.daily import DailyScraper

#------------------------------------------------------------------------------------------
# Use Traditional method instead of TaskFlow API
# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
#
#------------------------------------------------------------------------------------------
host_name = "10.186.17.206" # "10.186.17.206"
conn_params = {
"host" : host_name,
"database" : "Fin_proj",
"user" : "postgres",
"password" : "nckumark"
}

scraper = DailyScraper(conn_params)
func_map = {'daily_stock_price': scraper.daily_stock_price_update, 'daily_institution_trade':scraper.daily_three_insti_buying_update,
            'daily_total_trade': scraper.daily_total_trade_update, 'daily_institution_total_trade': scraper.daily_institution_total_trade_update,
            'daily_total_return_index': scraper.daily_total_return_index_update}

default_args = {
    'owner': 'Mark Chen',
    'retries': 3,
    'email':['l501l501l@gmail.com'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='Daily_stock_update',
    default_args=default_args,
    start_date=pendulum.datetime(2023, 6, 12, 2, 0 , tz="America/Indiana/Indianapolis"),
    schedule_interval='0 2 * * *',
    tags=['daily', 'data'],
) as dag:
    
    task1 = PythonOperator(
        task_id='Get_outdated_tables',
        python_callable=scraper.get_outdated_tables,
    )
    table_list = scraper.get_outdated_tables()

    for table in table_list:
        task = PythonOperator(
            task_id=f'Update_{table}',
            python_callable=func_map[table],
            op_kwargs = {'sleep_sec':30}
            )
        task1 >> task