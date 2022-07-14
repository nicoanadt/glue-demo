import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="source_sqlserver12",
    table_name="data_dbo_items",
    additional_options = {"jobBookmarkKeys":["updated_date"],"jobBookmarkKeysSortOrder":"asc"},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("item_id", "string", "item_id", "string"),
        ("style", "string", "style", "string"),
        ("updated_date", "timestamp", "updated_date", "timestamp"),
        ("category", "string", "category", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node SQL
SqlQuery0 = """
select *, unix_timestamp() job_ts from myDataSource

"""
SQL_node1657534734141 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": ApplyMapping_node2},
    transformation_ctx="SQL_node1657534734141",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://glue-test-ap-southeast-1-130835040051/data/sql12_items/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["job_ts"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="glue_demo", catalogTableName="sql12_items"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(SQL_node1657534734141)
job.commit()
