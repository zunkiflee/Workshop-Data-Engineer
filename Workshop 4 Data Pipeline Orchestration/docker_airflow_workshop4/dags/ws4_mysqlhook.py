from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import sqlalchemy


MYSQL_CONNECTION = 'mysql_default'
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"


mysql_output_path = '/home/airflow/data/transaction_data_merged.parquet'
conversion_rate_output_path = '/home/airflow/data/conversion_rate.parquet'
final_output_path = '/home/airflow/data/workshop4_output.parquet'


default_args = {
    'owner': 'zunkiflee',
}


@dag(default_args=default_args, schedule_interval='@once', start_date=days_ago(1), tags=['workshop_pipeline'])
def pipeline_mysqlhook():

    @task()
    def get_data_from_mysql(output_path):
        mysqlserver = MySqlHook(MYSQL_CONNECTION)

        product = mysqlserver.get_pandas_df(sql='SELECT * FROM r2de3.product')
        customer = mysqlserver.get_pandas_df(sql='SELECT * FROM r2de3.customer')
        transaction = mysqlserver.get_pandas_df(sql='SELECT * FROM r2de3.transaction')

        merged_transaction = transaction.merge(
            product, how='left', left_on='ProductNo',
            right_on='ProductNo').merge(
                customer, how='left', left_on='CustomerNo', right_on='CustomerNo')
        
        merged_transaction.to_parquet(output_path, index=False)
        print(f"Output to {output_path}")

    @task()
    def get_conversion_rate(output_path):
        r = requests.get(CONVERSION_RATE_URL)
        result_conversion_rate = r.json()
        df_conversion_rate = pd.DataFrame(result_conversion_rate)
        df_conversion_rate = df_conversion_rate.drop(columns=['id'])

        df_conversion_rate['date'] = pd.to_datetime(df_conversion_rate['date'])
        df_conversion_rate.to_parquet(output_path, index=False)
        print(f"Output to {df_conversion_rate}")

    @task
    def merge_data(transaction_path, conversion_rate_path, output_path):
        transaction = pd.read_parquet(transaction_path)
        conversion_rate = pd.read_parquet(conversion_rate_path)

        final_df = transaction.merge(conversion_rate, how='left', left_on='Date', right_on='date')

        final_df['total_amount'] = final_df['Price'] * final_df["Quantity"]
        final_df['thb_amount'] = final_df['total_amount'] * final_df['gbp_thb']

        final_df = final_df.drop(['date', 'gbp_thb'], axis=1)

        final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
            'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']

        final_df.to_parquet(output_path, index=False)
        print(f"Output to {output_path}")

    
    t1 = get_data_from_mysql(output_path=mysql_output_path)
    t2 = get_conversion_rate(output_path=conversion_rate_output_path)
    t3 = merge_data(transaction_path=mysql_output_path, 
                    conversion_rate_path=conversion_rate_output_path, 
                    output_path=final_output_path
                    )


    [t1, t2] >> t3

pipeline_mysqlhook()