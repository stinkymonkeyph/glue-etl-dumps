
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

spark = SparkSession.builder \
    .appName("Iceberg to Postgres Insert") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://poc-emr-coffee/iceberg_storage") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# Read the data from the Iceberg table
table_name = "glue_catalog.poc_emr_iceberg.app_user_landing"
app_user_df = spark.table(table_name)

# Format the address as "city, state, country, zipcode"
app_user_df = app_user_df.withColumn(
    "address", concat_ws(", ", col("city"), col(
        "state"), col("country"), col("zipcode"))
)

# Select only the required columns and match the clients table schema
clients_df = app_user_df.select(
    col("id"),
    col("first_name"),
    col("middle_name"),
    col("last_name"),
    col("zipcode").alias("zip_code"),
    col("address"),
    col("created_at"),
    col("updated_at")
)

# PostgreSQL connection details

postgres_url = ""
postgres_user = ""
postgres_password = ""

# Write to the PostgreSQL database
clients_df.write \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", "clients") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", "1000") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
