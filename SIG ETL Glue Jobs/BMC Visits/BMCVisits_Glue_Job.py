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

# Read the input CSV file
input_file_key = args['input_path']
file_content = read_s3_file(input_file_key)
df = pd.read_csv(StringIO(file_content), delimiter=',', dtype=str)

############################################################################
######################### Data Transformations Begins ######################
############################################################################

column_names = ['Patient MRN', 'Patient First Name', 'Patient Last Name', 'Patient Date of Birth', 
                'Department Name', 'Provider', 'Provider_Npi', 'Appointment Date', 'Appointment Time',
                'Appointment Type', 'Appointment Status']

df.columns = column_names


df['File Name'] = os.path.basename(input_file_key)
df['Load Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
df['Patient Date of Birth'] = pd.to_datetime(df['Patient Date of Birth'], format='%Y%m%d').dt.strftime('%m-%d-%Y')
df['Appointment Date'] = pd.to_datetime(df['Appointment Date'], format='%m/%d/%Y').dt.strftime('%m-%d-%Y')

         
############################################################################
######################### Data Transformations Ends ########################
############################################################################

# Write the transformed data to S3 as CSV
output_buffer = StringIO()
df.to_csv(output_buffer, index=False)
output_buffer.seek(0)

s3_resource = boto3.resource('s3')
output_bucket = "bmc-tfs-bucket-030063318327"
output_key = f"DecryptedFiles/test_transform/{os.path.basename(input_file_key)}"
s3_resource.Object(output_bucket, output_key).put(Body=output_buffer.getvalue())

job.commit()