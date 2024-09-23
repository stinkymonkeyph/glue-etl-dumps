from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
import boto3
import logging
import sys
import os


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Database and table names
        database_name = "poc_emr_iceberg"
        table_name = "employee"

        # Output S3 bucket
        output_bucket = "poc-emr-coffee"
        temp_output_key_prefix = "temp/employees"
        output_key_prefix = "business_output/departments"

        # Iceberg warehouse S3 path
        iceberg_warehouse_s3_path = "s3://poc-emr-coffee/iceberg_storage"

        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("EMR Serverless Iceberg Employees Export") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", iceberg_warehouse_s3_path) \
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized.")

        # Read data from Iceberg table
        table_full_name = f"glue_catalog.{database_name}.{table_name}"
        df = spark.table(table_full_name)
        logger.info(f"Read data from Iceberg table '{table_full_name}'.")

        # Get distinct departments
        departments = [row.department for row in df.select(
            "department").distinct().collect()]
        logger.info(f"Found departments: {departments}")

        # Initialize boto3 S3 client
        s3 = boto3.client('s3')

        # Process each department
        for department in departments:
            logger.info(f"Processing department: {department}")

            # Filter by department
            df_department = df.filter(col("department") == department)

            # Select required columns and create 'employee_name'
            df_selected = df_department.select(
                concat_ws(" ", col("first_name"), col(
                    "last_name")).alias("employee_name"),
                col("nationality"),
                col("salary"),
                col("performance_score")
            )

            # Repartition to optimize write performance
            df_selected = df_selected.repartition(1)

            # Temporary output path for department CSV
            temp_output_key = f"""{
                temp_output_key_prefix}/{department}/employees.csv"""
            temp_output_s3_path = f"s3://{output_bucket}/{temp_output_key}"

            # Write the result to a temporary directory
            df_selected.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(temp_output_s3_path)
            logger.info(f"""Data written to temporary path: {
                        temp_output_s3_path}""")

            # List the files in the temporary folder to get the actual CSV file
            temp_output_prefix = os.path.dirname(temp_output_key)
            temp_files = s3.list_objects_v2(
                Bucket=output_bucket, Prefix=temp_output_prefix)

            # Find the CSV file that Spark generated
            csv_file_key = None
            for obj in temp_files.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                    csv_file_key = obj['Key']
                    break

            if not csv_file_key:
                raise Exception(
                    f"No CSV file found for department {department}.")

            # Final output path for department CSV
            final_output_key = f"""{
                output_key_prefix}/{department}/employees.csv"""
            final_output_s3_path = f"s3://{output_bucket}/{final_output_key}"

            # Copy the CSV file to the final destination
            s3.copy_object(
                Bucket=output_bucket,
                CopySource={'Bucket': output_bucket, 'Key': csv_file_key},
                Key=final_output_key
            )
            logger.info(f"""Copied CSV file for department {
                        department} to '{final_output_s3_path}'.""")

            # Delete the temporary files
            for obj in temp_files.get('Contents', []):
                s3.delete_object(Bucket=output_bucket, Key=obj['Key'])
            logger.info(f"""Temporary files for department {
                        department} deleted.""")

    except Exception as e:
        logger.error(
            "An error occurred during the Spark job execution.", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
