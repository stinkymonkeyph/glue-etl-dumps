import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

db_username = "username"
db_password = "password"
db_url = "db_url"
table_name = "schema.tablename"
jdbc_driver_name = "driver"
s3_outut = "s3://output"

df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", db_url).option(
    "dbtable", table_name).option("user", db_username).option("password", db_password).load()

job.init(args['JOB_NAME'], args)

dynamic_df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")

datasink4 = glueContext.write_dynamic_frame.from_options(frame=dynamic_df, connection_type="s3", connection_options={
                                                         "path": s3_outut}, format="csv", transformation_ctx="datasink4")

job.commit()
