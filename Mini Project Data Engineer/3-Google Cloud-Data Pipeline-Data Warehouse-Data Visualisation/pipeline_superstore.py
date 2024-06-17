# Importing Modules
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import pandas as pd


# data source 
url = 'gs://asia-southeast1-finalworksh-972469c5-bucket/data/superstore.csv'

# path data ใน airflow สำหรับเก็บช้อมูลในโฟลเดอร์ data
final_output_path = '/home/airflow/gcs/data/final_superstore.csv'


# function get data
# รับค่าจาก t1 ค่า push_path เก็บค่าชุดข้อมูล final_path เก็บค่า path
def get_data(push_path, final_path):
    print('Load Data')

    # อ่านข้อมูลไฟล์ csv และตั้ง index column 
    df_superstore = pd.read_csv(url, index_col='Order ID')

    # rename columns
    df_superstore.rename(columns={'Order ID': 'Order_ID',
                           'Order Date': 'Order_Date', 'Ship Date': 'Ship_Date',
                           'Ship Mode': 'Ship_Mode', 'Customer ID': 'Customer_ID',
                           'Customer Name': 'Customer_Name', 'Postal Code': 'Postal_Code',
                           'Product ID': 'Product_ID', 'Sub-Category': 'Sub_Category',
                           'Product Name': 'Product_Name'}, inplace=True)

    # แปลงคอลัมน์ Order_Date, Ship_Date ใหเป็น date
    df_superstore['Order_Date'] = pd.to_datetime(df_superstore['Order_Date'])
    df_superstore['Ship_Date'] = pd.to_datetime(df_superstore['Ship_Date'])
    
    # save to csv 
    df_superstore.to_csv(final_path, index=False)
    print(f"Output to {final_path}")


with DAG(
    dag_id='superstore_dag',
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['load_data_bucket']
) as dag:


    # Tsak 
    # เรียกใช้ฟังก์ชัน get_data เพื่อไปดึงข้อมูล
    # op_kwargs ส่งตัวแปรที่เก็บ url ของข้อมูลและ path 
    t1 = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        op_kwargs={
            'push_path': url,
            'final_path': final_output_path
        }
    )

    # t2 ใช้ GCSToBigQueryOperator เก็บข้อมูลใน Data Warehosue
    t2 = GCSToBigQueryOperator(
        task_id = 'gcs_to_bq',
        bucket = 'asia-southeast1-finalworksh-972469c5-bucket', # link bucket
        source_objects = ['data/final_superstore.csv'],
        destination_project_dataset_table = 'marine-bay-288006.datastore.store', # bigquery path
        skip_leading_rows=1,
        schema_fields=[
            {
                "mode": "NULLABLE",
                "name": "Order_ID",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Order_Date",
                "type": "DATE"
            },
            {
                "mode": "NULLABLE",
                "name": "Ship_Date",
                "type": "DATE"
            },
            {
                "mode": "NULLABLE",
                "name": "Ship_Mode",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Customer_ID",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Customer_Name",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Segment",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Country",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "City",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "State",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Postal_Code",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Region",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Product_ID",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Category",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Sub_Category",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Product_Name",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Sales",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Quantity",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Discount",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Profit",
                "type": "FLOAT"
            }
        ],
    write_disposition='WRITE_APPEND',
    )

    # Setting up Dependencies
    t1 >> t2