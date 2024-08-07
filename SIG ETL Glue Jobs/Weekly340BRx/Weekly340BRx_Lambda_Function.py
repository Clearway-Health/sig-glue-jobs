import boto3
import logging
import time
import os
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not sns_topic_arn:
        logger.error("SNS_TOPIC_ARN environment variable is not set")
        return {
            'statusCode': 500,
            'body': 'SNS_TOPIC_ARN environment variable is not set'
        }
    
    sns_client = boto3.client('sns', region_name='us-east-1')
    
    try:
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        
        logger.info("Processing file: s3://%s/%s", s3_bucket, s3_key)
        
        if s3_key.startswith('DecryptedFiles/SIG-CWH-TFS/Weekly340BRx'):
            return process_file(s3_bucket, s3_key, sns_client, sns_topic_arn)
        else:
            logger.info("File does not match pattern, no action taken.")
            return {
                'statusCode': 200,
                'body': 'File does not match pattern, no action taken.'
            }
    except KeyError as e:
        return handle_error(e, sns_client, sns_topic_arn, "KeyError", 400)
    except Exception as e:
        return handle_error(e, sns_client, sns_topic_arn, "Exception", 500)

def process_file(s3_bucket, s3_key, sns_client, sns_topic_arn):
    glue_client = boto3.client('glue', region_name='us-east-1')
    try:
        job_run_id = start_glue_job(glue_client, s3_bucket, s3_key)
        status, job_status = wait_for_job_completion(glue_client, job_run_id)
        return handle_job_result(status, job_status, s3_key, sns_client, sns_topic_arn)
    except Exception as e:
        return handle_glue_error(e, sns_client, sns_topic_arn)

def start_glue_job(glue_client, s3_bucket, s3_key):
    response = glue_client.start_job_run(
        JobName='SIG_Weekly340Rx_GLue_Job',
        Arguments={
            '--input_path': f's3://{s3_bucket}/{s3_key}'
        }
    )
    job_run_id = response['JobRunId']
    logger.info(f"Glue job started: {job_run_id}")
    logger.info(f"Input path passed to Glue job: s3://{s3_bucket}/{s3_key}")
    return job_run_id

def wait_for_job_completion(glue_client, job_run_id):
    while True:
        job_status = glue_client.get_job_run(JobName='SIG_Weekly340Rx_GLue_Job', RunId=job_run_id)
        status = job_status['JobRun']['JobRunState']
        logger.info("Job status: %s", status)
        
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            return status, job_status
        time.sleep(30)

def handle_job_result(status, job_status, s3_key, sns_client, sns_topic_arn):
    sns_subject = f'Glue Job Completed - SIG_Weekly340Rx_GLue_Job - Status: {status}'
    sns_message = f'The Glue job SIG_Weekly340Rx_GLue_Job has {status} for file {s3_key}.'
    
    if status == 'SUCCEEDED':
        logger.info(f'Glue job SIG_Weekly340Rx_GLue_Job completed successfully for file {s3_key}.')
    else:
        error_message = job_status['JobRun'].get('ErrorMessage', 'No error message provided')
        logger.error(f'Glue job SIG_Weekly340Rx_GLue_Job failed for file {s3_key}. Status: {status}. Error: {error_message}')
        sns_message += f' Error: {error_message}'

    publish_sns_message(sns_client, sns_topic_arn, sns_subject, sns_message)
    return {
        'statusCode': 200,
        'body': f'Job {status}'
    }

def handle_glue_error(e, sns_client, sns_topic_arn):
    error_type = type(e).__name__
    logger.error(f"{error_type}: {e}")
    sns_subject = 'Glue Job Error - SIG_Weekly340Rx_GLue_Job'
    sns_message = f"""
    Error occurred while processing Glue job SIG_Weekly340Rx_GLue_Job.
    Error type: {error_type}
    Error message: {str(e)}
    """
    publish_sns_message(sns_client, sns_topic_arn, sns_subject, sns_message)
    return {
        'statusCode': 500,
        'body': f"{error_type}: {e}"
    }

def handle_error(e, sns_client, sns_topic_arn, error_type, status_code):
    logger.error(f"{error_type}: {e}")
    sns_subject = 'Lambda Function Error - SIG_Weekly340Rx_GLue_Job'
    sns_message = f"""
    Error occurred in Lambda function for SIG_Weekly340Rx_GLue_Job.
    Error type: {error_type}
    Error message: {str(e)}
    """
    publish_sns_message(sns_client, sns_topic_arn, sns_subject, sns_message)
    return {
        'statusCode': status_code,
        'body': f"{error_type}: {e}"
    }

def publish_sns_message(sns_client, sns_topic_arn, subject, message):
    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info(f"Message published to SNS. MessageId: {response['MessageId']}")
    except Exception as e:
        logger.error(f"Failed to publish message to SNS: {e}")
        logger.error(f"SNS Topic ARN: {sns_topic_arn}")
        logger.error(f"Subject: {subject}")
        logger.error(f"Message: {message}")