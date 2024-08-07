import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pandas as pd
from io import StringIO
from datetime import datetime
import os

# Get job name and input path from Lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to read S3 file content
def read_s3_file(s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

try:
    # Read the input CSV file
    input_file_key = args['input_path']
    print(f"Reading file from: {input_file_key}")
    file_content = read_s3_file(input_file_key)
    print("File content read successfully.")
    
    # Create DataFrame
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
    print("DataFrame created successfully.")
    
    column_names = [
        "MRN", "lastname", "firstname", "MI", "dob", "sex", "phone", "Addr1",
        "Addr2", "City", "State", "zip", "insurance1", "InsuranceType", "Medication",
        "Class", "NDC", "Quantity", "Refills", "Dx1", "orderer", "NPI", "Site",
        "SiteName", "SiteAddr", "Method", "Status", "RxDate", "PharmacyName",
        "PharmacyAddr", "PharmacyCity", "PharmacyState", "PharmacyZip"
    ]

    df.columns = column_names

    # Data Transformations
    df['dob'] = pd.to_datetime(df['dob']).apply(lambda x: x.strftime('%m-%d-%Y'))
    df['RxDate'] = pd.to_datetime(df['RxDate']).apply(lambda x: x.strftime('%m-%d-%Y'))
    df['phone'] = df['phone'].apply(lambda x: x.replace('-', ''))
    df['File Name'] = os.path.basename(input_file_key)
    df['Load Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    print("Data transformations completed.")

    # Write the transformed data to S3 as CSV
    output_buffer = StringIO()
    df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)
    print("CSV data prepared for upload.")
    
    s3_resource = boto3.resource('s3')
    output_bucket = "bmc-tfs-bucket-030063318327"
    #output_key = f"DecryptedFiles/test_transform/{os.path.basename(input_file_key)}"
    output_key = f"DecryptedFiles/test_transform/{os.path.splitext(os.path.basename(input_file_key))[0]}.csv"
    
    s3_resource.Object(output_bucket, output_key).put(Body=output_buffer.getvalue())
    print(f"File successfully uploaded to {output_bucket}/{output_key}")

except Exception as e:
    print(f"Error: {e}")

job.commit()
