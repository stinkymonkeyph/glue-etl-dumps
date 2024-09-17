import sys
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
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
s3_prefix = f"{project_name}/{year}/{month}/{day}/{topic_folder}/silver/"
s3_final_prefix = f"{project_name}/{year}/{month}/{day}/{topic_folder}/gold/"
unique_temp_id = str(uuid.uuid4())
temp_folder = f"{project_name}/{year}/{month}/{day}/temp_{unique_temp_id}/"

# Generate the final report file name with a unique ID
unique_id = str(uuid.uuid4())  # Generate a unique identifier
final_file_name = f"report_summary_{unique_id}.json"

# Initialize Glue job
job.init(args['JOB_NAME'], args)

# Step 1: List objects in the S3 bucket to get the latest uploaded file from the silver folder
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

# Cast the 'price' column to double for computations
df = df.withColumn("price", df["price"].cast("double"))

# Step 2: Generate the report based on the file content
items_shipped_count = df.filter(df["status"] == "Shipped").count()
items_delivered_count = df.filter(df["status"] == "Delivered").count()
items_pending_count = df.filter(df["status"] == "Pending").count()
average_price = df.agg({"price": "avg"}).collect()[0]["avg(price)"]
total_orders = df.count()

report = {
    "items_shipped_count": items_shipped_count,
    "items_delivered_count": items_delivered_count,
    "items_pending_count": items_pending_count,
    "average_price": average_price,
    "total_orders": total_orders
}

print(f"Report generated: {report}")

# Step 3: Write the report to the temp folder as JSON, ensuring it is written as a single file
temp_report_output = f"s3://{s3_bucket}/{temp_folder}"
report_df = spark.createDataFrame([report])

# Repartition the DataFrame to a single partition to ensure only one file is written
report_df.repartition(1).write.json(temp_report_output)

# Ensure that the temp file is written
print(f"Waiting for the temp report file to be written to {
      temp_report_output}")
time.sleep(20)  # Increase sleep time to ensure full write

# Step 4: Get the exact temp file written
response_temp = s3_client.list_objects_v2(
    Bucket=s3_bucket,
    Prefix=temp_folder
)

if 'Contents' in response_temp:
    # Get the latest file from the temp folder
    latest_temp_file = max(
        response_temp['Contents'], key=lambda x: x['LastModified'])['Key']
    print(f"Latest temp report file found: {latest_temp_file}")

    # Check the file size to ensure it is not 0 bytes
    temp_file_size = s3_client.head_object(
        Bucket=s3_bucket, Key=latest_temp_file)['ContentLength']
    if temp_file_size == 0:
        raise Exception(f"Error: The temp report file {
                        latest_temp_file} is 0 bytes.")

    # Define the final cleaned file path in the gold folder
    final_s3_output = f"{s3_final_prefix}{final_file_name}"

    # Copy the temporary report to the final location
    s3_client.copy_object(
        Bucket=s3_bucket,
        CopySource={'Bucket': s3_bucket, 'Key': latest_temp_file},
        Key=final_s3_output
    )

    print(f"Copied report to: s3://{s3_bucket}/{final_s3_output}")

    # Delete the temporary file (from the temp folder)
    s3_client.delete_object(
        Bucket=s3_bucket,
        Key=latest_temp_file
    )

    print(f"Deleted temporary report: s3://{s3_bucket}/{latest_temp_file}")

    # Step 5: Clean up the entire temp folder after processing
    print(f"Cleaning up temp folder: {temp_folder}")
    temp_objects = s3_client.list_objects_v2(
        Bucket=s3_bucket, Prefix=temp_folder)
    if 'Contents' in temp_objects:
        for obj in temp_objects['Contents']:
            print(f"Deleting temp file: {obj['Key']}")
            s3_client.delete_object(Bucket=s3_bucket, Key=obj['Key'])
    print(f"Temp folder {temp_folder} cleaned up.")

else:
    print(f"No temp report files found in {temp_folder}")

# Commit the Glue job
job.commit()
