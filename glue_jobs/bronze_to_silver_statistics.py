import sys
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_S3_PATH = "s3://data-ytproject-raw-useast1-dev/youtube/raw_statistics/"
SILVER_DATABASE = "data-ytproject-cleansed"
SILVER_TABLE = "silver_statistics"
SILVER_S3_PATH = "s3://dataproject-youtubecleansed-useast1-dev/silver/statistics/"

# Read directly with Spark's native CSV reader instead of Glue's DynamicFrame
# CSV reader. Glue's reader (JacksonReader) throws a FatalException on a
# handful of malformed/unbalanced-quote rows in RUvideos.csv; Spark's reader
# with mode="DROPMALFORMED" just skips those specific bad rows instead of
# failing the whole job. `region` comes through automatically as a Hive
# partition column since the files live under region=xx/ folders.
df = (spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", '"')
      .option("escape", '"')
      .option("mode", "DROPMALFORMED")
      .csv(RAW_S3_PATH))

df = (df
      .withColumn("category_id", F.col("category_id").cast("long"))
      .withColumn("views", F.col("views").cast("long"))
      .withColumn("likes", F.col("likes").cast("long"))
      .withColumn("dislikes", F.col("dislikes").cast("long"))
      .withColumn("comment_count", F.col("comment_count").cast("long"))
      .withColumn("comments_disabled", F.col("comments_disabled").cast("boolean"))
      .withColumn("ratings_disabled", F.col("ratings_disabled").cast("boolean"))
      .withColumn("video_error_or_removed", F.col("video_error_or_removed").cast("boolean")))

# Silver-layer cleansing: dedupe exact re-ingestions of the same video/day/region
df = df.dropDuplicates(["video_id", "trending_date", "region"])
df = df.filter(F.col("video_id").isNotNull() & F.col("views").isNotNull() & (F.col("views") > 0))

# Derived metric
df = df.withColumn(
    "engagement_rate",
    F.when(F.col("views") > 0, (F.col("likes") + F.col("comment_count")) / F.col("views")).otherwise(F.lit(0.0))
)

print(f"Silver statistics record count after cleansing: {df.count()}")

silver_dyf = DynamicFrame.fromDF(df.coalesce(1), glueContext, "silver_dyf")

sink = glueContext.getSink(
    connection_type="s3", path=SILVER_S3_PATH,
    enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"]
)
sink.setFormat("glueparquet", compression="snappy")
sink.setCatalogInfo(catalogDatabase=SILVER_DATABASE, catalogTableName=SILVER_TABLE)
sink.writeFrame(silver_dyf)

job.commit()
