
import sys
import uuid  # Import UUID for unique filenames
import time  # For introducing delay to ensure temp file is written
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime
import boto3

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Database connection details
db_username = ""
db_password = ""
db_url = "jdbc:postgresql://host:6543/postgres"
jdbc_driver_name = "org.postgresql.Driver"

# Initialize Boto3 S3 client
s3_client = boto3.client('s3')

# S3 bucket and folder structure
s3_bucket = "lake-poc"  # The bucket we used earlier
project_name = "online-shopping"  # Updated project name
topic_folder = "orderitems"  # Topic folder updated to "orderitems"

# Get the current date for folder structure
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%B")  # Full month name (e.g., "September")
day = now.strftime("%d")

# Dynamically generate the folder path based on the current date
s3_prefix = f"{project_name}/{year}/{month}/{day}/{topic_folder}/bronze/"

# Separate temporary folder for storing the temporary file
temp_folder = f"{project_name}/{year}/{month}/{day}/temp/"

temp_file_name = "orderitems-temp.csv"
final_file_prefix = "orderitems"

# Generate final file name with date-time format and UUID for uniqueness
current_time = now.strftime("%Y-%m-%d-%H-%M-%S")
unique_id = str(uuid.uuid4())  # Generate a unique identifier
final_file_name = f"{final_file_prefix}-{current_time}-{unique_id}.csv"

# Initialize Glue job
job.init(args['JOB_NAME'], args)

# SQL query to perform join on the database side
sql_query = """
    SELECT 
        oi.OrderItemID,
        oi.Quantity,
        oi.UnitPrice,
        oi.Status,
        o.OrderDate,
        o.TotalAmount,
        c.FirstName,
        c.LastName,
        c.Email,
        p.ProductName,
        p.ProductType,
        p.Price
    FROM 
        public.orderitems oi
    JOIN 
        public.orders o ON oi.OrderID = o.OrderID
    JOIN 
        public.customers c ON o.CustomerID = c.CustomerID
    JOIN 
        public.products p ON oi.ProductID = p.ProductID
"""

# Read data from PostgreSQL using the SQL join query
joined_df = glueContext.read \
    .format("jdbc") \
    .option("driver", jdbc_driver_name) \
    .option("url", db_url) \
    .option("query", sql_query) \
    .option("user", db_username) \
    .option("password", db_password) \
    .load()

# Convert the joined DataFrame back to a DynamicFrame
joined_dynamic_df = DynamicFrame.fromDF(
    joined_df, glueContext, "joined_dynamic_df")

# Write DynamicFrame to S3 with a temporary filename in a separate temp folder
temp_s3_output = f"s3://{s3_bucket}/{temp_folder}{temp_file_name}"
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=joined_dynamic_df,
    connection_type="s3",
    connection_options={"path": temp_s3_output},
    format="csv",
    transformation_ctx="datasink4"
)

# Introduce a short delay to ensure the temp file is written to S3
time.sleep(5)  # 5 seconds delay; adjust as needed

# List objects in the S3 bucket with the prefix (to find the temporary file)
response = s3_client.list_objects(Bucket=s3_bucket, Prefix=temp_folder)

# Check if there are any objects under the prefix
if 'Contents' in response:
    # Sort the objects by the 'LastModified' timestamp in descending order
    sorted_objects = sorted(
        response["Contents"], key=lambda obj: obj['LastModified'], reverse=True)

    # Get the latest object (which is the most recently uploaded temp file)
    latest_object = sorted_objects[0]
    name = latest_object["Key"]

    # Prepare the copy source with the correct bucket and key
    copy_source = {'Bucket': s3_bucket, 'Key': name}

    # Copy the file to the final folder with the correct file name format
    final_s3_output = f"{s3_prefix}{final_file_name}"
    s3_client.copy_object(
        Bucket=s3_bucket,
        CopySource=copy_source,  # Correct CopySource format
        Key=final_s3_output
    )

    print(f"Copied file to: s3://{s3_bucket}/{final_s3_output}")

    # Delete the temporary file after copying
    s3_client.delete_object(
        Bucket=s3_bucket,
        Key=name
    )
    print(f"Deleted temporary file: s3://{s3_bucket}/{name}")
else:
    print(f"No objects found in the specified S3 prefix: {temp_folder}")

# Commit Glue job
job.commit()
