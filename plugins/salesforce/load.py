import pandas as pd
import pytz
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging


def InsertIntoPostgres(csv_file_path: str, table_name: str, schema: str):
    """
    Inserts data from a CSV file into a specified PostgreSQL table.

    Args:
        csv_file_path (str): Path to the csv file containing the data.
        table_name (str): Name of the table to insert data into.
        schema (str): Schema of the table.

    Returns:
        None
    """
    df = pd.read_csv(csv_file_path)
    df.drop(columns=['Unnamed: 0'], inplace=True)
    if df.empty:
        logging.warning("The DataFrame is empty. No data to insert.")
        return

    inserted_at = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')
    df['inserted_at'] = inserted_at
    logging.info(f"DataFrame head: {df.head(10)}")
    logging.info(f"DataFrame shape: {df.shape}")
    logging.info(f"DataFrame columns: {df.columns}")
    logging.info(f"DataFrame dtypes: {df.dtypes}")

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        for index, row in df.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(['%s'] * len(row))
            insert_stmt = f'INSERT INTO {schema}.{table_name} ({columns}) VALUES ({values})'
            cursor.execute(insert_stmt, tuple(row))
        conn.commit()
        logging.info(f"Data successfully inserted into {schema}.{table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting data into {schema}.{table_name}: {e}")
    finally:
        cursor.close()
        conn.close()