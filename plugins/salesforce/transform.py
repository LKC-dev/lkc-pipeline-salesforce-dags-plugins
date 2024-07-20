import pandas as pd
import logging

def TransformSilver(columns_to_rename: dict, table_schema: dict) -> pd.DataFrame:
    """
    Transforms a JSON file into a DataFrame with specified schema and renames columns.

    Args:
        columns_to_rename (dict): A dictionary mapping original column names to new names.
        table_schema (dict): A dictionary mapping column names to their data types.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    df = pd.read_json('dags/salesforce/2024_01_01_16h_salesforce_account.json')
    df = df.rename(columns=columns_to_rename)
    
    for column, column_type in table_schema.items():
        if column in df:
            if column_type in ['int', 'float']:
                try:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    df[column] = df[column].replace('', None)
                    df[column] = df[column].replace('nan', pd.NA)
                    df[column] = df[column].replace('NaN', pd.NA)
                except ValueError as e:
                    logging.error(f'Error transforming column {column} to {column_type}: {e}')

            elif column_type == 'datetime':
                try:
                    formats = [
                        '%d/%m/%Y', 
                        '%Y/%m/%d', 
                        '%Y-%m-%dT%H:%M:%S%z', 
                        '%Y-%m-%dT%H:%M:%S.%f%z', 
                        '%Y-%m-%dT%H:%M:%S', 
                        '%Y-%m-%d',
                        '%Y-%m-%dT%H:%M:%S'
                    ]
                    
                    sample_value = df[column].dropna().iloc[0]
                    for fmt in formats:
                        try:
                            pd.to_datetime(sample_value, format=fmt)
                            detected_format = fmt
                            break
                        except ValueError:
                            detected_format = None
                    
                    if detected_format:
                        df[column] = pd.to_datetime(df[column], format=detected_format, errors='coerce').dt.floor('s').astype('datetime64[us]')
                    else:
                        logging.warning(f"Unable to parse column '{column}' with any of the supported datetime formats.")
                except (ValueError, TypeError) as e:
                    logging.error(f'Error transforming column {column} to datetime: {e}')

            elif column_type == 'str':
                try:
                    df[column] = df[column].astype('string')
                    df[column] = df[column].replace('nan', pd.NA)
                except ValueError as e:
                    logging.error(f'Error transforming column {column} to string: {e}')

            elif column_type == 'bool':
                try:
                    df[column] = df[column].astype(bool)
                except ValueError as e:
                    logging.error(f'Error transforming column {column} to bool: {e}')
        else:
            logging.warning(f"Skipping, column '{column}' not found in the DataFrame.")

    df = df.replace('', None)    
    df = df.where(pd.notnull(df), None)

    df.to_csv('dags/salesforce/data.csv')
    logging.info("DataFrame successfully transformed and saved to 'dags/salesforce/data.csv'")