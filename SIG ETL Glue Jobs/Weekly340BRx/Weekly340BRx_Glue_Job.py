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

def get_secret():
    secret_name = "cwh-glue-secrets/snowflake-credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def main():
    print("Starting Glue job")
    try:
        # Get Snowflake credentials from Secrets Manager
        snowflake_conn_params = get_secret()
        print("Fetched Snowflake credentials")

        # Snowflake connection details
        snowflake_conn_params.update({
            'account': 'xbb27476.us-east-1',  # account details
            'warehouse': 'COMPUTE_WH',  # Provided Snowflake warehouse name
            'database': 'CLIENT_SIG',  # Your Snowflake database name
            'schema': 'STG_DATA'  # Your Snowflake schema name
        })

        conn = None

        try:
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
            def read_s3_file(s3_path):
                s3 = boto3.client('s3')
                bucket, key = s3_path.replace("s3://", "").split("/", 1)
                obj = s3.get_object(Bucket=bucket, Key=key)
                return obj['Body'].read().decode('utf-8')

            print(f"Reading file from: {input_file_key}")
            file_content = read_s3_file(input_file_key)
            print("File content read successfully")

            # Create DataFrame
            df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
            print("DataFrame created successfully")

            # Ensure column names match the Snowflake table
            column_names = [
                "MRN", "LASTNAME", "FIRSTNAME", "MI", "DOB", "SEX", "PHONE", "ADDR1",
                "ADDR2", "CITY", "STATE", "ZIP", "INSURANCE1", "INSURANCETYPE", "MEDICATION",
                "CLASS", "NDC", "QUANTITY", "REFILLS", "DX1", "ORDERER", "NPI", "SITE",
                "SITENAME", "SITEADDR", "METHOD", "STATUS", "RXDATE", "PHARMACYNAME",
                "PHARMACYADDR", "PHARMACYCITY", "PHARMACYSTATE", "PHARMACYZIP",
            ]
            df.columns = column_names

            # Data Transformations
            df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
            df['RXDATE'] = pd.to_datetime(df['RXDATE'], errors='coerce').apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
            df['PHONE'] = df['PHONE'].apply(lambda x: x.replace('-', ''))
            df['FILE_NAME'] = os.path.basename(input_file_key)
            df['LOAD_TIMESTAMP'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print("Data transformations completed")

            # Insert DataFrame into Snowflake using write_pandas
            success, nchunks, nrows, _ = write_pandas(conn, df, 'SIG_WEEKLY340RX', quote_identifiers=True)
            print(f"Data successfully uploaded to Snowflake table SIG_WEEKLY340RX: {success}, {nchunks}, {nrows}")

        except Exception as e:
            print(f"Error in data processing: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    except Exception as e:
        print(f"Error in Glue job: {e}")

    print("Glue job completed")

if __name__ == "__main__":
    main()
