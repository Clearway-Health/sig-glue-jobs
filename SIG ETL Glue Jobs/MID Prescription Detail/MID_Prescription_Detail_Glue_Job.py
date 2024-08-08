import sys
import boto3
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from io import StringIO
from datetime import datetime
import os
import re
import json
from awsglue.utils import getResolvedOptions

# Get the Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path'])
input_file_key = args['input_path']

print("Starting Glue job")
try:
    # Fetch Snowflake credentials from AWS Secrets Manager
    secret_name = "cwh-glue-secrets/snowflake-credentials"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    snowflake_conn_params = json.loads(secret)

    print("Fetched Snowflake credentials")

    # Snowflake connection details
    snowflake_conn_params.update({
        'account': 'xbb27476.us-east-1',  # account details
        'warehouse': 'COMPUTE_WH',  # Provided Snowflake warehouse name
        'database': 'CLIENT_SIG',  # Your Snowflake database name
        'schema': 'STG_DATA'  # Your Snowflake schema name
    })

    print(f"Processing file: {input_file_key}")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_conn_params['user'],
        password=snowflake_conn_params['password'],
        account=snowflake_conn_params['account'],
        warehouse=snowflake_conn_params['warehouse'],
        database=snowflake_conn_params['database'],
        schema=snowflake_conn_params['schema']
    )
    cursor = conn.cursor()
    print("Successfully connected to Snowflake")

    # Truncate the table
    cursor.execute("TRUNCATE TABLE CLIENT_SIG.STG_DATA.SIG_MID_PRESCRIPTION_DETAIL")
    print("Table truncated")

    # Read the input CSV file from S3
    s3 = boto3.client('s3')
    bucket, key = input_file_key.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = obj['Body'].read().decode('utf-8')

    print("File content read successfully")
    
    df = pd.read_csv(StringIO(file_content), delimiter=',', dtype=str)

############################################################################
######################### Data Transformations Begins ######################
############################################################################

    # Ensure column names match the Snowflake table
    column_names = [
    'PATIENT_MRN',
    'PATIENT_LAST_NAME',
    'PATIENT_FIRST_NAME',
    'PATIENT_MI',
    'PATIENT_DOB',
    'REF_NUMBER',
    'DATE_FILLED',
    'RX_NUMBER',
    'NDC',
    'DRUG_NAME',
    'QUANTITY',
    'DS',
    'PRIMARY_PAID',
    'SECONDARY_PAID',
    'TERTIARY_PAID',
    'PATIENT_PAID',
    'TOTAL',
    'ACQ',
    'PRIMARY',
    'PRIMARY_PATIENT_ID',
    'PRIMARY_BIN',
    'PRIMARY_PCN',
    'PRIMARY_GROUP',
    'SECONDARY',
    'SECONDARY_PATIENT_ID',
    'SECONDARY_BIN',
    'SECONDARY_PCN',
    'SECONDARY_GROUP',
    'PRESCRIBER_LAST_NAME',
    'PRESCRIBER_FIRST_NAME',
    'PRESCRIBER_NPI']
    
    df.columns = column_names

    # Data Transformations

    # Function to convert currency strings to float
    def currency_to_float(x):
        if isinstance(x, str):
            return pd.to_numeric(x.replace('$', '').replace(',', ''), errors='coerce')
        return x

    # Function to convert quantity strings to float
    def quantity_to_float(x):
        if isinstance(x, str):
            return pd.to_numeric(x.replace(',', ''), errors='coerce')
        return x

    # Apply the currency_to_float function to relevant columns
    currency_columns = ['PRIMARY_PAID', 'SECONDARY_PAID', 'TERTIARY_PAID', 'PATIENT_PAID', 'TOTAL', 'ACQ']
    for col in currency_columns:
        df[col] = df[col].apply(currency_to_float)

    # Convert QUANTITY to float
    df['QUANTITY'] = df['QUANTITY'].apply(quantity_to_float)

    df['FILE_NAME'] = os.path.basename(input_file_key)
    df['LOAD_TIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print("Data transformations completed")

    # Print data types to verify transformations
    print(df.dtypes)

    # Debug prints
    print("QUANTITY column head:")
    print(df['QUANTITY'].head())
    print("QUANTITY column dtype:", df['QUANTITY'].dtype)
    print("Number of null values in QUANTITY:", df['QUANTITY'].isnull().sum())
    
############################################################################
######################### Data Transformations Ends ########################
############################################################################

    # Insert DataFrame into Snowflake using write_pandas
    success, nchunks, nrows, _ = write_pandas(conn, df, 'SIG_MID_PRESCRIPTION_DETAIL', quote_identifiers=True)
    print(f"Data successfully uploaded to Snowflake table SIG_MID_PRESCRIPTION_DETAIL: {success}, {nchunks}, {nrows}")

except Exception as e:
    print(f"Error in data processing: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()

print("Glue job completed")
