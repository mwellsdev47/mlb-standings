# todo: implement Class structure
# todo: header

# Project ----------------------------------------
from config.config import BUCKET_NAME, S3_RAW_PATH
from config.aws_config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# 3rd Party-------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("MLBPipeline")
    # make sure the right S3A libs are on the classpath
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .getOrCreate()
)
spark.conf.set("spark.sql.debug.maxToStringFields", "1000")

# Set data types
schema = StructType([
    StructField("Rk", IntegerType(),  True),    # Rank in the standings
    StructField("Tm", StringType(),  True),     # Team name

    StructField("W",  IntegerType(), True),     # Wins
    StructField("L",  IntegerType(), True),     # Losses
    StructField("W-L%", DoubleType(), True),    # Win percentage = Wins ÷ (Wins + Losses)
    StructField("Strk", StringType(), True),    # Current streak

    StructField("R", DoubleType(), True),       # Runs scored per game
    StructField("RA", DoubleType(), True),      # Runs allowed per game
    StructField("Rdiff", DoubleType(), True),   # Run differential = R − RA
    StructField("SOS", DoubleType(), True),     # Strength of Schedule
    StructField("SRS", DoubleType(), True),     # Simple Rating System: combines Rdiff + SOS
    StructField("pythWL", StringType(), True),  # Pythagorean win-loss: expected record based on R/RA
    StructField("Luck", IntegerType(), True),   # Difference between actual W-L and pythWL

    StructField("vEast", StringType(), True),   # Record vs East division
    StructField("vCent", StringType(), True),   # Record vs Central division
    StructField("vWest", StringType(), True),   # Record vs West division
    StructField("Inter", StringType(), True),   # Record in Interleague play

    StructField("Home", StringType(), True),    # Home record
    StructField("Road", StringType(), True),    # Road record
    StructField("ExInn", StringType(), True),   # Extra innings record
    StructField("1Run", StringType(), True),    # Record in one-run games

    StructField("vRHP", StringType(), True),    # Record vs right-handed pitchers
    StructField("vLHP", StringType(), True),    # Record vs left-handed pitchers

    StructField("≥.500", StringType(), True),   # Record vs teams at or above .500 win percentage
    StructField("<.500", StringType(), True),   # Record vs teams below .500

    StructField("last10", StringType(), True),  # Record over last 10 games
    StructField("last20", StringType(), True),  # Record over last 20 games
    StructField("last30", StringType(), True),  # Record over last 30 games
])

# detects S3A settings that contain a suffix that hadoop does not accept. Use error message and update last conditional.
# conf = spark._jsc.hadoopConfiguration()
# it = conf.iterator()
# while it.hasNext():
#     e = it.next()
#     k, v = e.getKey(), e.getValue()
#     if k.startswith("fs.s3a") and "24h" in v or (k.startswith("fs.s3a") and v.endswith("h")):
#         print(k, "=", v)

hc = spark._jsc.hadoopConfiguration()
hc.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
hc.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
hc.set("fs.s3a.aws.credentials.provider",
       "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# override any duration-with-suffix values can’t parse. Use commented loop above if future issues arise.
hc.set("fs.s3a.threads.keepalivetime", "60")            # sec
hc.set("fs.s3a.connection.timeout", "20000")            # ms
hc.set("fs.s3a.connection.establish.timeout", "30000")  # ms
hc.set("fs.s3a.multipart.purge.age", "24")              # hr

try:
    # Read CSV from S3
    df = spark.read.csv(
        f"s3a://{BUCKET_NAME}/{S3_RAW_PATH}",
        header=True,
        inferSchema=True
    )
    # todo: convert to log
    print(f"Successfully read CSV from s3://{BUCKET_NAME}/{S3_RAW_PATH}")

    # Write to Parquet in S3
    df.write.parquet(f"s3a://{BUCKET_NAME}/transformed/standings.parquet", mode="overwrite")
    # todo: convert to log
    print(f"Successfully wrote Parquet to s3://{BUCKET_NAME}/transformed/standings.parquet")

except Exception as e:
    print(f"Error during ETL process: {e}")
    raise

# Stop Spark session
spark.stop()
