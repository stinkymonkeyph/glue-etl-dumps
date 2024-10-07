
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Parse arguments


def get_args():
    parser = argparse.ArgumentParser(description="Column mapping script")
    parser.add_argument("--column_mapping", type=str,
                        help="JSON string for column mapping", required=True)
    parser.add_argument("--s3_input_path", type=str,
                        help="S3 input path for the CSV file", required=True)
    return parser.parse_args()


# Get column mappings and input path from arguments
args = get_args()
column_mapping = json.loads(args.column_mapping)
s3_input_path = args.s3_input_path  # Dynamic S3 input path

# Initialize Spark session with the correct Iceberg configuration
spark = SparkSession.builder \
    .appName("CSV to Iceberg Merge Insert") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://poc-emr-coffee/iceberg_storage") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# Load CSV from dynamic S3 input path
source_df = spark.read.csv(s3_input_path, header=True)

# Apply column mapping to the DataFrame
mapped_columns = []
for src_col, dest_cols in column_mapping.items():
    if isinstance(dest_cols, list):
        # If a source column maps to multiple target columns
        for dest_col in dest_cols:
            mapped_columns.append(col(src_col).alias(dest_col))
    else:
        # If a source column maps to a single target column
        mapped_columns.append(col(src_col).alias(dest_cols))

# Apply the mapped columns
mapped_df = source_df.select(*mapped_columns)

# Cast created_at and updated_at to timestamp (assuming the source column is a string date format)
if "created_at" in mapped_df.columns:
    mapped_df = mapped_df.withColumn(
        "created_at", col("created_at").cast("timestamp"))

if "updated_at" in mapped_df.columns:
    mapped_df = mapped_df.withColumn(
        "updated_at", col("updated_at").cast("timestamp"))

# Create a temp view to use SQL
mapped_df.createOrReplaceTempView("mapped_temp_view")

# Define the Iceberg table name
table_name = "glue_catalog.poc_emr_iceberg.app_user_landing"

# Perform the MERGE INTO operation for conditional inserts
spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING mapped_temp_view AS source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
      INSERT *
""")

# Stop Spark session
spark.stop()
