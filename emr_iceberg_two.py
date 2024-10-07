
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType
from pyspark.sql.functions import year, date_format, when, col, trim, udf
import hashlib

import logging
logger = logging.getLogger("my_spark_logger")


# Function to hash PII using SHA-256
def hash_pii(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()


def main():
    # Database and table names
    database_name = "poc_emr_iceberg"
    table_name = "user_service_data"  # Table name

    # Source S3 paths (adjust as necessary)
    user_data_source_bucket = "poc-emr-coffee"
    user_data_source_key = "source/raw/everest/data.csv"
    reverse_geocode_source_key = "source/raw/geocode/reverse_geocode.csv"

    user_data_s3_path = f"s3://{user_data_source_bucket}/{user_data_source_key}"
    reverse_geocode_s3_path = f"""s3://{
        user_data_source_bucket}/{reverse_geocode_source_key}"""

    # Iceberg warehouse S3 path
    iceberg_warehouse_s3_path = "s3://poc-emr-coffee/iceberg_storage"

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("EMR Serverless Iceberg Example") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", iceberg_warehouse_s3_path) \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}")

    # Define the schema for the user service data
    user_data_schema = StructType([
        StructField("Everest ID", StringType(), True),
        StructField("Visit Timestamp", TimestampType(), True),
        StructField("First Name", StringType(), True),
        StructField("Middle Name", StringType(), True),
        StructField("Last Name", StringType(), True),
        StructField("Preferred Name", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Phone", StringType(), True),
        StructField("Address 1", StringType(), True),
        StructField("Address 2", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Zipcode", StringType(), True),
        StructField("Date of Birth", StringType(), True),
        StructField("SP - Meals", DoubleType(), True),
        StructField("SP - Respite(shelter)", DoubleType(), True),
        StructField("SP - Shop", DoubleType(), True),
        StructField("SP - Washer/Dryer", DoubleType(), True),
        StructField("SP - Other", DoubleType(), True),
        StructField("SP - Other Entry", StringType(), True),
        StructField("NA - Shelters", DoubleType(), True),
        StructField("NA - Low-cost housing", DoubleType(), True),
        StructField("NA - Home repair/maintenance", DoubleType(), True),
        StructField("NA - Rent assistance", DoubleType(), True),
        StructField("NA - Mortgage assistance", DoubleType(), True),
        StructField(
            "NA - Landlord/Tenant issues Contacts, Other housing & shelter", StringType(), True),
        StructField("NA - Housing/Shelter Other", DoubleType(), True),
        StructField("NA - Housing/Shelter Other Entry", StringType(), True),
        StructField("NA - Help buying food", DoubleType(), True),
        StructField("NA - Food pantries", DoubleType(), True),
        StructField("NA - Soup kitchens & Meals to-go", DoubleType(), True),
        StructField("NA - Feeding children", DoubleType(), True),
        StructField("NA - Home-delivered meals", DoubleType(), True),
        StructField("NA - Holiday meals", DoubleType(), True),
        StructField("NA - Food Contacts", StringType(), True),
        StructField("NA - Food Others", DoubleType(), True),
        StructField("NA - Food Others Entry", StringType(), True),
        StructField("NA - Electric", DoubleType(), True),
        StructField("NA - Gas", DoubleType(), True),
        StructField("NA - Water", DoubleType(), True),
        StructField("NA - Heating fuel", DoubleType(), True),
        StructField("NA - Trash collection", DoubleType(), True),
        StructField("NA - Utility payment plans", DoubleType(), True),
        StructField("NA - Utility deposit assistance", DoubleType(), True),
        StructField("NA - Disconnection protection", DoubleType(), True),
        StructField("NA - Phone/Internet", DoubleType(), True),
        StructField("NA - Utilities Contacts", StringType(), True),
        StructField("NA - Utilities Others", DoubleType(), True),
        StructField("NA - Utilities Others Entry", StringType(), True),
        StructField("NA - Health insurance", DoubleType(), True),
        StructField("NA - Medical expense assistance", DoubleType(), True),
        StructField("NA - Medical provider", DoubleType(), True),
        StructField("NA - Dental", DoubleType(), True),
        StructField("NA - Eye care", DoubleType(), True),
        StructField("NA - Prescription medication", DoubleType(), True),
        StructField("NA - Nursing home and adult care", DoubleType(), True),
        StructField("NA - Death related", DoubleType(), True),
        StructField("NA - Public health and safety", DoubleType(), True),
        StructField("NA - COVID testing", DoubleType(), True),
        StructField("NA - COVID vaccination", DoubleType(), True),
        StructField("NA - All other COVID", DoubleType(), True),
        StructField("NA - Other health services", DoubleType(), True),
        StructField("NA - Healthcare And COVID 19 Contacts",
                    StringType(), True),
        StructField("NA - Health and COVID 19 Others", DoubleType(), True),
        StructField("NA - Health and COVID 19 Others Entry",
                    StringType(), True),
        StructField("New User", BooleanType(), True)
    ])

    # Register the UDF for hashing
    hash_udf = udf(hash_pii, StringType())

    # Read user service data
    user_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(user_data_schema) \
        .load(user_data_s3_path)

    # Apply the UDF to hash PII columns
    user_df = user_df.withColumn("First Name", hash_udf(col("First Name"))) \
                     .withColumn("Last Name", hash_udf(col("Last Name"))) \
                     .withColumn("Email", hash_udf(col("Email"))) \
                     .withColumn("Phone", hash_udf(col("Phone"))) \
                     .withColumn("Date of Birth", hash_udf(col("Date of Birth")))

    # Add a year and month name column for partitioning by "Visit Timestamp"
    user_df = user_df.withColumn(
        "visit_year", year(user_df["Visit Timestamp"]))
    user_df = user_df.withColumn("visit_month", date_format(
        user_df["Visit Timestamp"], "MMMM"))  # Add month name

    # Define schema for reverse_geocode.csv
    reverse_geocode_schema = StructType([
        StructField("Country", StringType(), True),
        StructField("Country_Code", StringType(), True),
        StructField("Geo_City", StringType(), True),
        StructField("Geo_State", StringType(), True),
        StructField("State_Code", StringType(), True),
        StructField("Zipcode", StringType(), True)
    ])

    # Read reverse geocode data
    reverse_geocode_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(reverse_geocode_schema) \
        .load(reverse_geocode_s3_path) \

    # Ensure Zipcode columns are trimmed for consistency
    user_df = user_df.withColumn("Zipcode", trim(col("Zipcode")))

    # Print schema and first few rows using logger
    logger.info("Schema of reverse_geocode_df:")
    reverse_geocode_df.printSchema()

    logger.info("Preview of reverse_geocode_df:")
    reverse_geocode_df.show(10)

    logger.info("Preview of user_df:")
    user_df.show(10)

    # More readable join
    enhanced_df = user_df.join(reverse_geocode_df, user_df.Zipcode == reverse_geocode_df.Zipcode, "left_outer") \
        .drop(reverse_geocode_df.Zipcode) \
        .withColumn("City", when(col("City").isNull() | (col("City") == ""), col("Geo_City")).otherwise(col("City"))) \
        .withColumn("State", when(col("State").isNull() | (col("State") == ""), col("Geo_State")).otherwise(col("State"))) \
        .select(
        "Everest ID", "Visit Timestamp", "First Name", "Middle Name", "Last Name", "Preferred Name",
            "Email", "Phone", "Address 1", "Address 2", "City", "State", "Zipcode", "Date of Birth",
            "SP - Meals", "SP - Respite(shelter)", "SP - Shop", "SP - Washer/Dryer", "SP - Other",
            "SP - Other Entry", "NA - Shelters", "NA - Low-cost housing", "NA - Home repair/maintenance",
            "NA - Rent assistance", "NA - Mortgage assistance", "NA - Landlord/Tenant issues Contacts, Other housing & shelter",
            "NA - Housing/Shelter Other", "NA - Housing/Shelter Other Entry", "NA - Help buying food",
            "NA - Food pantries", "NA - Soup kitchens & Meals to-go", "NA - Feeding children", "NA - Home-delivered meals",
            "NA - Holiday meals", "NA - Food Contacts", "NA - Food Others", "NA - Food Others Entry",
            "NA - Electric", "NA - Gas", "NA - Water", "NA - Heating fuel", "NA - Trash collection",
            "NA - Utility payment plans", "NA - Utility deposit assistance", "NA - Disconnection protection",
            "NA - Phone/Internet", "NA - Utilities Contacts", "NA - Utilities Others", "NA - Utilities Others Entry",
            "NA - Health insurance", "NA - Medical expense assistance", "NA - Medical provider", "NA - Dental",
            "NA - Eye care", "NA - Prescription medication", "NA - Nursing home and adult care", "NA - Death related",
            "NA - Public health and safety", "NA - COVID testing", "NA - COVID vaccination", "NA - All other COVID",
            "NA - Other health services", "NA - Healthcare And COVID 19 Contacts", "NA - Health and COVID 19 Others",
            "NA - Health and COVID 19 Others Entry", "New User", "Country", "Country_Code", "State_Code", "visit_year", "visit_month"
    )

    # Register the DataFrame as a temp view
    enhanced_df.createOrReplaceTempView("enhanced_user_service_temp_view")

    # Create Iceberg table with the new columns
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{table_name} (
        `Everest ID` STRING,
        `Visit Timestamp` TIMESTAMP,
        `First Name` STRING,
        `Middle Name` STRING,
        `Last Name` STRING,
        `Preferred Name` STRING,
        `Email` STRING,
        `Phone` STRING,
        `Address 1` STRING,
        `Address 2` STRING,
        `City` STRING,
        `State` STRING,
        `Zipcode` STRING,
        `Date of Birth` STRING,
        `SP - Meals` DOUBLE,
        `SP - Respite(shelter)` DOUBLE,
        `SP - Shop` DOUBLE,
        `SP - Washer/Dryer` DOUBLE,
        `SP - Other` DOUBLE,
        `SP - Other Entry` STRING,
        `NA - Shelters` DOUBLE,
        `NA - Low-cost housing` DOUBLE,
        `NA - Home repair/maintenance` DOUBLE,
        `NA - Rent assistance` DOUBLE,
        `NA - Mortgage assistance` DOUBLE,
        `NA - Landlord/Tenant issues Contacts, Other housing & shelter` STRING,
        `NA - Housing/Shelter Other` DOUBLE,
        `NA - Housing/Shelter Other Entry` STRING,
        `NA - Help buying food` DOUBLE,
        `NA - Food pantries` DOUBLE,
        `NA - Soup kitchens & Meals to-go` DOUBLE,
        `NA - Feeding children` DOUBLE,
        `NA - Home-delivered meals` DOUBLE,
        `NA - Holiday meals` DOUBLE,
        `NA - Food Contacts` STRING,
        `NA - Food Others` DOUBLE,
        `NA - Food Others Entry` STRING,
        `NA - Electric` DOUBLE,
        `NA - Gas` DOUBLE,
        `NA - Water` DOUBLE,
        `NA - Heating fuel` DOUBLE,
        `NA - Trash collection` DOUBLE,
        `NA - Utility payment plans` DOUBLE,
        `NA - Utility deposit assistance` DOUBLE,
        `NA - Disconnection protection` DOUBLE,
        `NA - Phone/Internet` DOUBLE,
        `NA - Utilities Contacts` STRING,
        `NA - Utilities Others` DOUBLE,
        `NA - Utilities Others Entry` STRING,
        `NA - Health insurance` DOUBLE,
        `NA - Medical expense assistance` DOUBLE,
        `NA - Medical provider` DOUBLE,
        `NA - Dental` DOUBLE,
        `NA - Eye care` DOUBLE,
        `NA - Prescription medication` DOUBLE,
        `NA - Nursing home and adult care` DOUBLE,
        `NA - Death related` DOUBLE,
        `NA - Public health and safety` DOUBLE,
        `NA - COVID testing` DOUBLE,
        `NA - COVID vaccination` DOUBLE,
        `NA - All other COVID` DOUBLE,
        `NA - Other health services` DOUBLE,
        `NA - Healthcare And COVID 19 Contacts` STRING,
        `NA - Health and COVID 19 Others` DOUBLE,
        `NA - Health and COVID 19 Others Entry` STRING,
        `New User` BOOLEAN,
        `Country` STRING,
        `Country_Code` STRING,
        `State_Code` STRING,
        `visit_year` INT,
        `visit_month` STRING
    )
    USING ICEBERG
    PARTITIONED BY (visit_year, visit_month)
    """
    spark.sql(create_table_sql)

    # Insert enhanced data into the Iceberg table
    spark.sql(f"""
    INSERT OVERWRITE glue_catalog.{database_name}.{table_name}
    SELECT * FROM enhanced_user_service_temp_view
    """)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
