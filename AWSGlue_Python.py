# Data Masking of PII fields while moving Data from Production to Lower environments.
import sys
import hashlib
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Set up Glue job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read raw data from S3
raw_df = spark.read.option("header", "true").csv("s3://your-bucket/raw-data/")

# Step 2: Define a UDF to scramble the AccountID
def scramble_account_id(account_id):
    if account_id:
        return hashlib.sha256(account_id.encode('utf-8')).hexdigest()
    return None

scramble_udf = udf(scramble_account_id, StringType())

# Step 3: Apply the scrambling function
scrambled_df = raw_df.withColumn("AccountID", scramble_udf(raw_df["AccountID"]))

# Step 4: Write the transformed data to S3 in Parquet format
scrambled_df.write.mode("overwrite").parquet("s3://your-bucket/processed-data/")

# Commit the job
job.commit()
