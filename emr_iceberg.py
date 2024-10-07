

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import year, when


def main():
    # Database and table names
    database_name = "poc_emr_iceberg"
    table_name = "employee"

    # Source S3 bucket and key
    source_bucket = "poc-emr-coffee"
    source_key = "source/raw/employee_data/sample_dataset_employee.csv"
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
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}")

    # Define the schema for the employee data
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("department", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("hire_date", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("education_level", StringType(), True),
        StructField("performance_score", IntegerType(), True),
        StructField("manager_id", IntegerType(), True)
    ])

    # Read employee data from CSV
    df_employee = spark.read.format("csv") \
        .option("header", "true") \
        .schema(employee_schema) \
        .load(source_s3_path)

    # Replace nationality "USA" with "United States" and "UK" with "United Kingdom"
    df_employee = df_employee.withColumn(
        "nationality",
        when(df_employee["nationality"] == "USA", "United States")
        .when(df_employee["nationality"] == "UK", "United Kingdom")
        .otherwise(df_employee["nationality"])
    )

    # Add a year column for partitioning
    df_employee = df_employee.withColumn(
        "hire_year", year(df_employee.hire_date))

    # Load the country latitude/longitude data from a pre-created CSV in S3
    country_latlong_path = "s3://poc-emr-coffee/source/raw/geocode/country_lat_long.csv"

    country_schema = StructType([
        StructField("Country", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True)
    ])

    df_country_latlong = spark.read.format("csv") \
        .option("header", "true") \
        .schema(country_schema) \
        .load(country_latlong_path)

    # Join employee data with country latitude/longitude based on nationality and country
    df_employee_enriched = df_employee.join(df_country_latlong, df_employee.nationality == df_country_latlong.Country, "left_outer") \
        .drop("Country")

    # Register the enriched DataFrame as a temp view
    df_employee_enriched.createOrReplaceTempView("employee_temp_view")

    # Create employee table if not exists, partitioned by department, job_title, and hire_year
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{table_name} (
        employee_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        department STRING,
        job_title STRING,
        salary INT,
        hire_date DATE,
        nationality STRING,
        age INT,
        gender STRING,
        education_level STRING,
        performance_score INT,
        manager_id INT,
        hire_year INT,
        latitude DOUBLE,
        longitude DOUBLE
    )
    USING ICEBERG
    PARTITIONED BY (department, job_title, hire_year)
    """
    spark.sql(create_table_sql)

    # Insert enriched data into the employee table
    spark.sql(f"""
    INSERT OVERWRITE glue_catalog.{database_name}.{table_name}
    SELECT * FROM employee_temp_view
    """)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
