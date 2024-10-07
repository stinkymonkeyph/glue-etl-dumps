
from pyspark.sql import SparkSession

import logging
logger = logging.getLogger("my_spark_logger")


def main():
    # Database and table names
    database_name = "poc_emr_iceberg"
    table_name = "user_service_data"  # Table name

    # Iceberg warehouse S3 path
    iceberg_warehouse_s3_path = "s3://poc-emr-coffee/iceberg_storage"

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Evolve Schema in Iceberg Table - Add visit_month") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", iceberg_warehouse_s3_path) \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Add the 'visit_month' column to the table schema
    spark.sql(f"""
        ALTER TABLE glue_catalog.{database_name}.{table_name}
        ADD COLUMN visit_month STRING
    """)

    # Update the partition specification to include 'visit_month'
    spark.sql(f"""
        ALTER TABLE glue_catalog.{database_name}.{table_name}
        ADD PARTITION FIELD date_format(`Visit Timestamp`, 'MMMM') AS visit_month
    """)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
