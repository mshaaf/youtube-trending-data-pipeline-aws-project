import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

SILVER_DATABASE = "data-ytproject-cleansed"
SILVER_TABLE = "silver_statistics"
REFERENCE_TABLE = "raw_statistics_reference_data"
GOLD_DATABASE = "data-ytproject-gold"
GOLD_S3_ROOT = "s3://dataproject-youtubecleansed-useast1-dev/gold"

silver_df = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE, table_name=SILVER_TABLE, transformation_ctx="silver_dyf"
).toDF()

try:
    ref_df = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE, table_name=REFERENCE_TABLE, transformation_ctx="ref_dyf"
    ).toDF().select(
        F.col("id").cast("long").alias("category_id"),
        F.col("snippet_title").alias("category_name")
    )
    silver_df = silver_df.join(ref_df, on="category_id", how="left")
except Exception as e:
    print(f"Reference data unavailable, proceeding without category names: {e}")
    silver_df = silver_df.withColumn("category_name", F.lit(None).cast("string"))

silver_df = silver_df.withColumn("category_name", F.coalesce(F.col("category_name"), F.lit("Unknown")))


def write_gold_table(df, table_name):
    print(f"{table_name} record count: {df.count()}")
    dyf = DynamicFrame.fromDF(df.coalesce(1), glueContext, table_name)
    sink = glueContext.getSink(
        connection_type="s3", path=f"{GOLD_S3_ROOT}/{table_name}/",
        enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"]
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.setCatalogInfo(catalogDatabase=GOLD_DATABASE, catalogTableName=table_name)
    sink.writeFrame(dyf)


# Gold 1: daily regional rollup
trending_analytics = silver_df.groupBy("region", "trending_date").agg(
    F.countDistinct("video_id").alias("video_count"),
    F.sum("views").alias("total_views"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.countDistinct("channel_title").alias("distinct_channels"),
    F.countDistinct("category_name").alias("distinct_categories")
)
write_gold_table(trending_analytics, "trending_analytics")

# Gold 2: channel performance, ranked within region
channel_base = silver_df.groupBy("channel_title", "region").agg(
    F.sum("views").alias("total_views"),
    F.countDistinct("video_id").alias("video_count"),
    F.min("trending_date").alias("first_trending_date"),
    F.max("trending_date").alias("last_trending_date")
)
channel_window = Window.partitionBy("region").orderBy(F.desc("total_views"))
channel_analytics = channel_base.withColumn("channel_rank", F.row_number().over(channel_window))
write_gold_table(channel_analytics, "channel_analytics")

# Gold 3: category view-share per region/day
category_base = silver_df.groupBy("category_name", "region", "trending_date").agg(
    F.sum("views").alias("total_views"),
    F.countDistinct("video_id").alias("video_count")
)
share_window = Window.partitionBy("region", "trending_date")
category_analytics = category_base.withColumn(
    "view_share_pct", F.round(F.col("total_views") / F.sum("total_views").over(share_window) * 100, 2)
)
write_gold_table(category_analytics, "category_analytics")

job.commit()
