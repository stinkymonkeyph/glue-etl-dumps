from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


def main():
    # Database and table names
    database_name = "poc_emr_iceberg"
    table_name = "employee"

    # Source S3 bucket and key
    source_bucket = "poc-emr-coffee"
    source_key = "source/employee.csv"
    source_s3_path = f"s3://{source_bucket}/{source_key}"

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
        .getOrCreate()

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}")

    # Read data
    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(
            StructType([
                StructField("employee_id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("hire_date", DateType(), True)
            ])
    ) \
        .load(source_s3_path)

    # Register DataFrame as temp view
    df.createOrReplaceTempView("employee_temp_view")

    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{table_name} (
        employee_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        department STRING,
        salary INT,
        hire_date DATE
    )
    USING ICEBERG
    PARTITIONED BY (department)
    """
    spark.sql(create_table_sql)

    # Insert data into table
    spark.sql(f"""
    INSERT INTO glue_catalog.{database_name}.{table_name}
    SELECT * FROM employee_temp_view
    """)

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
