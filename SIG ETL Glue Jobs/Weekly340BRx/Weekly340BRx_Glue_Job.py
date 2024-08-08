import sys
import boto3
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from io import StringIO
from datetime import datetime
import os
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
    cursor.execute("TRUNCATE TABLE CLIENT_SIG.STG_DATA.SIG_WEEKLY340RX")
    print("Table truncated")

    # Read the input CSV file from S3
    s3 = boto3.client('s3')
    bucket, key = input_file_key.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = obj['Body'].read().decode('utf-8')

    print("File content read successfully")


############################################################################
######################### Data Transformations Begins ######################
############################################################################

    # Create DataFrame
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
    print("DataFrame created successfully")

    # Ensure column names match the Snowflake table
    column_names = [ 'MRN','LAST_NAME','FIRST_NAME','MI','DOB','SEX','PHONE','ADDR1','ADDR2','CITY','STATE','ZIP','INSURANCE_1',
'INSURANCE_TYPE','MEDICATION','CLASS','NDC','QUANTITY','RE_FILLS','DX1','ORDERER','NPI','SITE','SITE_NAME','SITE_ADDR','METHOD','STATUS','RX_DATE','PHARMACY_NAME','PHARMACY_ADDR','PHARMACY_CITY','PHARMACY_STATE','PHARMACY_ZIP']
    
    df.columns = column_names

    # Data Transformations
    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
    df['RX_DATE'] = pd.to_datetime(df['RX_DATE'], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
    df['PHONE'] = df['PHONE'].apply(lambda x: x.replace('-', ''))
    df['FILE_NAME'] = os.path.basename(input_file_key)
    df['LOAD_TIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print("Data transformations completed")
    
############################################################################
######################### Data Transformations Ends ########################
############################################################################

    # Insert DataFrame into Snowflake using write_pandas
    success, nchunks, nrows, _ = write_pandas(conn, df, 'SIG_WEEKLY340RX', quote_identifiers=True)
    print(f"Data successfully uploaded to Snowflake table SIG_WEEKLY340RX: {success}, {nchunks}, {nrows}")

except Exception as e:
    print(f"Error in data processing: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()

print("Glue job completed")
