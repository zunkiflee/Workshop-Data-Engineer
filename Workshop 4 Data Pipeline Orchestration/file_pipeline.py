# Importing Modules
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
 
# connection server sql and api
MYSQL_CONNECTION = "mysql_default" 
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
 
# path data ใน airflow
mysql_output_path = "/home/airflow/gcs/data/audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/databook.csv"
 
# function data mysql
def get_data_from_mysql(transaction_path):
    # รับ transaction_path จาก t1 
 
    # เชื่อมต่อ MySQL
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
 
    # query จาก database ใช้ hook ที่สร้าง จะได้ pandas DataFrame
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")
 
    # รวม Merge audible_data และ audible_transaction
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")
 
    # save ไฟล์ csv transaction_path
    # '/home/airflow/gcs/data/audible_data_merged.csv'
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")
 
# โหลด api จาก url 
def get_conversion_rate(conversion_rate_path):
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
 
    # เปลี่ยน index เป็น data พร้อมชื่อคอลัมน์ว่า date
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"Output to {conversion_rate_path}")
 
# function รวม transaction_path, conversion_rate_path มา join
def merge_data(transaction_path, conversion_rate_path, output_path):
    # ทำการอ่านไฟล์ทั้ง 3 ไฟล์ ที่โยนเข้ามาจาก task 
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)
 
    # สร้างคอลัมน์ใหม่ data ใน transaction
    # แปลง transaction['date'] เป็น timestamp
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date
 
    # merge 2 datframe transaction, conversion_rate
    fianl_databook = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
     
    # ลบเครื่องหมาย $ ในคอลัมน์ Price และแปลงเป็น float
    fianl_databook['Price'] = fianl_databook.apply(lambda x: x["Price"].replace("$",""), axis=1)
    fianl_databook['Price'] = fianl_databook['Price'].astype(float)
 
    # สร้างคอลัมน์ใหม่ชื่อว่า THBPrice เอา price * conversion_rate
    fianl_databook['THBPrice'] = fianl_databook['Price'] * fianl_databook['conversion_rate']
    fianl_databook = fianl_databook.drop(["date", "book_id"], axis=1)
 
    # save ไฟล์ fianl_databook เป็น csv
    fianl_databook.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
 
# Instantiate a Dag
with DAG(
    dag_id="book_dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:
 
    # อธิบาย DAG 
    dag.doc_md = """ 
        Workshop Pipeline ETL Book 
    """
     
    # Task 
    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )
     
    t2 = PythonOperator(
        task_id="get_conversion_rate",
        python_callable=get_conversion_rate,
        op_kwargs={
            "conversion_rate_path": conversion_rate_output_path,
        },
    )
 
    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path,
            "output_path" : final_output_path,
        },
    )
     
    # Setting up Dependencies
    [t1, t2] >> t3