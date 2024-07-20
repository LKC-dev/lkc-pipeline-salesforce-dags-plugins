import logging
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError


def get_s3_file(bucket: str, source: str, table_source: str, process_date: str):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    landing_path = f'{source}/landing'
    
    formatted_date = datetime.strptime(process_date, '%Y-%m-%d').strftime('%Y-%m-%d')
    expected_filename = f"{formatted_date}_{table_source}.csv"
    expected_key = f"{landing_path}/{expected_filename}"

    logging.info(f'Expected key: {expected_key}')
    
    try:
        file_obj = s3_hook.get_key(expected_key, bucket_name=bucket)
        file_content = file_obj.get()['Body'].read()
        
        if len(file_content) == 0:
            logging.info(f"Skipping empty file: {expected_key}")
            return None
        
        return file_content

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error(f"File not found: {expected_key}")
        else:
            logging.error(f"Error retrieving file {expected_key}: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to process file {expected_key}: {e}")
        raise e