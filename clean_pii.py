
import sys
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime
import boto3
import time

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize Boto3 S3 client
s3_client = boto3.client('s3')

# S3 bucket and folder structure
s3_bucket = "lake-poc"
project_name = "online-shopping"
topic_folder = "orderitems"

# Get the current date for folder structure
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%B")
day = now.strftime("%d")

# Dynamically generate the folder paths based on the current date
s3_prefix = f"{project_name}/{year}/{month}/{day}/{topic_folder}/bronze/"
s3_final_prefix = f"{project_name}/{year}/{month}/{day}/{topic_folder}/silver/"
temp_folder = f"{project_name}/{year}/{month}/{day}/temp/"

# Generate cleaned file name with UUID for uniqueness
unique_id = str(uuid.uuid4())  # Generate a unique identifier
cleaned_file_name = f"orderitems_cleaned_{unique_id}.csv"

# Temporary file name in temp folder
temp_file_name = f"orderitems_cleaned_temp.csv"

# Initialize Glue job
job.init(args['JOB_NAME'], args)

# Wait for a few seconds to ensure files are written to S3
time.sleep(10)

# List objects in the S3 bucket to get the latest uploaded file from the orderitems/ folder
response = s3_client.list_objects_v2(
    Bucket=s3_bucket,
    Prefix=s3_prefix
)

if 'Contents' not in response or len(response['Contents']) == 0:
    raise Exception(
        f"No objects found in the specified S3 prefix: {s3_prefix}")

# Get the latest uploaded file (based on LastModified timestamp)
latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
latest_file_key = latest_file['Key']

# Read the latest file into a Spark DataFrame
latest_file_s3_path = f"s3://{s3_bucket}/{latest_file_key}"
df = spark.read.option("header", "true").csv(latest_file_s3_path)

# Drop the PII columns: FirstName, LastName, Email
cleaned_df = df.drop("FirstName", "LastName", "Email")

# Convert DataFrame to DynamicFrame
cleaned_dynamic_df = DynamicFrame.fromDF(
    cleaned_df, glueContext, "cleaned_dynamic_df")

# Write cleaned DynamicFrame to S3 with a temporary filename in the temp folder
temp_s3_output = f"s3://{s3_bucket}/{temp_folder}{temp_file_name}"
datasink = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_df,
    connection_type="s3",
    connection_options={"path": temp_s3_output},
    format="csv",
    transformation_ctx="datasink"
)

print(f"Temporary cleaned file saved at: {temp_s3_output}")

# Wait for the temp file to be written
time.sleep(10)

# List objects in the S3 bucket with the temp folder to ensure the temp file exists
response = s3_client.list_objects_v2(
    Bucket=s3_bucket,
    Prefix=temp_folder
)

if 'Contents' in response:
    # Get the first object (which is the temp file we just uploaded)
    temp_file = max(response['Contents'], key=lambda x: x['LastModified'])
    temp_file_key = temp_file['Key']

    # Define the final cleaned file path
    final_s3_output = f"{s3_final_prefix}{cleaned_file_name}"

    # Copy the file to the final folder with a new name
    s3_client.copy_object(
        Bucket=s3_bucket,
        CopySource={'Bucket': s3_bucket, 'Key': temp_file_key},
        Key=final_s3_output
    )

    print(f"Copied cleaned file to: s3://{s3_bucket}/{final_s3_output}")

    # Delete the temporary file (from the temp folder)
    s3_client.delete_object(
        Bucket=s3_bucket,
        Key=temp_file_key
    )

    print(f"Deleted temporary file: s3://{s3_bucket}/{temp_file_key}")
else:
    print(f"No objects found in the specified S3 prefix: {temp_folder}")

# Commit Glue job
job.commit()
