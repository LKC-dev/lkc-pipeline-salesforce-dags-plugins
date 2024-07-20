from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from utils.s3_functions import get_s3_file
from salesforce.transform import TransformSilver
from salesforce.load import InsertIntoPostgres
import yaml

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': None,
    'retries': 1,
}

with open('./dags/salesforce/settings/settings.yaml', 'r') as file:
    config = yaml.safe_load(file)

with DAG(
    'salesforce_accounts_pipeline',
    default_args=default_args,
    description='DAG to transform and load from S3 into PostgreSQL DW',
    schedule_interval=None,
    catchup=False,
) as dag:
    
    file_key = config['file_key']
    bucket_name = config['bucket_name']
    rename_columns = config['rename_columns']
    table_schema = config['table_schema']
    silver_table_name = config['silver_table_name']
    silver_schema = config['silver_schema']
    pickle_file_path = config['pickle_file_path']
    silver_create_schema = config['silver_create_schema']
    silver_create_table = config['silver_create_table']
    silver_deduplication_query = config['silver_deduplication_query']
    gold_table_name = config['gold_table_name']
    gold_schema = config['gold_schema']
    gold_create_schema = config['gold_create_schema']
    gold_create_table = config['gold_create_table']
    gold_deduplication_query = config['gold_deduplication_query']

    read_s3_file = PythonOperator(
        task_id='read_s3_file',
        python_callable=get_s3_file,
        op_kwargs={
            'bucket_name': bucket_name,
            'key': file_key,
        },
    )

    with TaskGroup('silver_tasks') as silver_tasks:
        transform_silver_task = PythonOperator(
            task_id='transform_silver',
            python_callable=TransformSilver,
            op_kwargs={
                'columns_to_rename': rename_columns,
                'table_schema': table_schema
            }
        )

        create_silver_schema_task = SQLExecuteQueryOperator(
            task_id='create_silver_schema',
            conn_id='postgres_default',
            sql=silver_create_schema
        )

        create_silver_table_task = SQLExecuteQueryOperator(
            task_id='create_silver_table',
            conn_id='postgres_default',
            sql=silver_create_table
        )

        load_to_silver_task = PythonOperator(
            task_id='load_to_silver',
            python_callable=InsertIntoPostgres,
            op_kwargs={
                'pickle_file_path': pickle_file_path,
                'table_name': silver_table_name,
                'schema': silver_schema
            }
        )

        deduplicate_silver_task = SQLExecuteQueryOperator(
            task_id='deduplicate_silver',
            conn_id='postgres_default',
            sql=silver_deduplication_query
        )

        transform_silver_task >> create_silver_schema_task >> create_silver_table_task >> load_to_silver_task >> deduplicate_silver_task

    with TaskGroup('gold_tasks') as gold_tasks:
        create_gold_schema_task = SQLExecuteQueryOperator(
            task_id='create_gold_schema',
            conn_id='postgres_default',
            sql=gold_create_schema
        )

        create_gold_table_task = SQLExecuteQueryOperator(
            task_id='create_gold_table',
            conn_id='postgres_default',
            sql=gold_create_table
        )

        load_to_gold_task = PythonOperator(
            task_id='load_to_gold',
            python_callable=InsertIntoPostgres,
            op_kwargs={
                'pickle_file_path': pickle_file_path,
                'table_name': gold_table_name,
                'schema': gold_schema
            }
        )

        deduplicate_gold_task = SQLExecuteQueryOperator(
            task_id='deduplicate_gold',
            conn_id='postgres_default',
            sql=gold_deduplication_query
        )

        create_gold_schema_task >> create_gold_table_task >> load_to_gold_task >> deduplicate_gold_task

    read_s3_file >> silver_tasks >> gold_tasks