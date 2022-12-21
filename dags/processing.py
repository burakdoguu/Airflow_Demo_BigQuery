from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from google.cloud import storage
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

PROJECT_ID = 'precise-blend-371110'
STAGING_DATASET = "airflow_demo"


def explode_json():
 
    data ='/opt/airflow/dags/resources/orders.json'

    df = pd.read_json(data,lines=True)
    review_dict = df.to_dict(orient="records")
    result = pd.json_normalize(review_dict,record_path=['lineitems'],meta=['event','messageid','userid','orderid'])
    result.to_csv('/opt/airflow/dags/resources/order_data.csv',index=False)
    #result.to_json('/opt/airflow/dags/resources/exploded_orders.json', orient='records', lines=True)


with DAG('send_to_gcs_file', start_date=datetime(2022,1,1),
    schedule_interval='@daily',catchup=False) as dag:

    upload_csv_file = LocalFilesystemToGCSOperator(
        task_id ="upload_csv_file",
        bucket='test_local_file',
        src='/opt/airflow/dags/resources/product-category-map.csv',
        dst='product-category-map.csv'
    )

    process_orders_data = PythonOperator(
        task_id='process_orders_data',
        python_callable=explode_json
    )

    reupload_exploded_orders_data = LocalFilesystemToGCSOperator(
        task_id ="reupload_exploded_orders_data",
        bucket='test_local_file',
        src='/opt/airflow/dags/resources/order_data.csv',
        dst='order_data.csv'
    )

    product_category_map_data_dataset = GCSToBigQueryOperator(
        task_id = 'productcategorymap_data_dataset',
        bucket = 'test_local_file',
        source_objects= ['product-category-map.csv'],
        destination_project_dataset_table= f'{PROJECT_ID}:{STAGING_DATASET}.product-category-map',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'productid', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'categoryid', 'type': 'STRING', 'mode': 'NULLABLE'},]

    )
    orders_data_dataset = GCSToBigQueryOperator(
        task_id = 'orders_data_dataset',
        bucket = 'test_local_file',
        source_objects= ['order_data.csv'],
        destination_project_dataset_table= f'{PROJECT_ID}:{STAGING_DATASET}.orders_data',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'productid', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'event', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'messageid', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'userid', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'orderid', 'type': 'INTEGER', 'mode': 'NULLABLE'},]
    )

    bigquery_execute_query = BigQueryExecuteQueryOperator(
        task_id="execute_query",
        sql="""
        CREATE OR REPLACE TABLE `precise-blend-371110.airflow_demo.top_10_product_category` (productid STRING, total_bought INTEGER,categoryid STRING)
        OPTIONS(
          description="Top ten words per Shakespeare corpus"
        ) AS
        SELECT productid, total_bought, categoryid FROM (
          SELECT productid, total_bought, categoryid,CAST(regexp_extract(categoryid, '[0-9]+') as INT64)
        FROM (
          SELECT 
          (ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY SUM(quantity) DESC)) as RN,
          productid, SUM(quantity) as total_bought,categoryid
          FROM (
            SELECT o.productid as productid,o.quantity as quantity,o.event,o.messageid,o.userid,o.orderid,c.categoryid as categoryid
            FROM `precise-blend-371110.airflow_demo.orders_data` o
            INNER JOIN `airflow_demo.product-category-map` c 
            ON o.productid = c.productid)
          GROUP BY productid, categoryid
        )
        WHERE RN <=10
        ORDER BY 4
        )
        """,
        use_legacy_sql=False
    )

    upload_csv_file >> process_orders_data >> reupload_exploded_orders_data >> product_category_map_data_dataset >> orders_data_dataset >> bigquery_execute_query
