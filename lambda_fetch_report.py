import json
import boto3
from datetime import datetime

# Initialize the S3 client
s3_client = boto3.client('s3')

# Constants for S3 bucket and folder structure
S3_BUCKET = "lake-poc"  # Replace with your bucket name
PROJECT_NAME = "online-shopping"  # Project folder in S3
TOPIC_FOLDER = "orderitems"  # Folder for reports
# Change if you have a different structure
REPORT_FOLDER_PREFIX = f"{PROJECT_NAME}/"


def lambda_handler(event, context):
    try:
        # Get the current date for the folder structure
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%B")
        day = now.strftime("%d")

        # Path to the report folder (adjust if your folder structure is different)
        report_prefix = f"{REPORT_FOLDER_PREFIX}{
            year}/{month}/{day}/{TOPIC_FOLDER}/gold/"

        # List objects in the S3 bucket under the report folder
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=report_prefix)

        # Check if any objects were found
        if 'Contents' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps("No reports found in the folder.")
            }

        # Find the latest JSON report by LastModified timestamp
        latest_file = max(response['Contents'],
                          key=lambda x: x['LastModified'])
        latest_report_key = latest_file['Key']
        print(f"Latest report file found: {latest_report_key}")

        # Fetch the content of the latest JSON report
        report_object = s3_client.get_object(
            Bucket=S3_BUCKET, Key=latest_report_key)
        report_content = report_object['Body'].read().decode('utf-8')

        # Return the latest report content
        return {
            "statusCode": 200,
            "body": report_content,
            "report_key": latest_report_key
        }

    except Exception as e:
        print(f"Error fetching the latest report: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}")
        }
