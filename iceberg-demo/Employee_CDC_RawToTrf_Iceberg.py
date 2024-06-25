import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, max

from pyspark.conf import SparkConf

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_job_catalog_warehouse'])
conf = SparkConf()

## Please make sure to pass runtime argument --iceberg_job_catalog_warehouse with value as the S3 path 
conf.set("spark.sql.catalog.job_catalog.warehouse", args['iceberg_job_catalog_warehouse'])
conf.set("spark.sql.catalog.job_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.job_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.job_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
conf.set("spark.sql.iceberg.handle-timestamp-without-timezone","true")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

## Read Input Table
IncrementalInputDyF = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://hudiglueblog-raws3bucket-peeeobltsx6i/human_resources/employee_details/"]}, transformation_ctx="AmazonS3_node1711950221114")

IncrementalInputDF = IncrementalInputDyF.toDF()

if not IncrementalInputDF.rdd.isEmpty():
    ## Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation 
    IDWindowDF = Window.partitionBy(IncrementalInputDF.emp_no).orderBy(IncrementalInputDF.update_ts_dms).rangeBetween(-sys.maxsize, sys.maxsize)
                  
    # Add new columns to capture first and last OP value and what is the latest timestamp
    inputDFWithTS= IncrementalInputDF.withColumn("max_op_date",max(IncrementalInputDF.update_ts_dms).over(IDWindowDF))
    
    # Filter out new records that are inserted, then select latest record from existing records and merge both to get deduplicated output 
    finalInputDF = inputDFWithTS.filter("update_ts_dms=max_op_date or update_ts_dms is null")

    # Register the deduplicated input as temporary table to use in Iceberg Spark SQL statements
    finalInputDF.createOrReplaceTempView("incremental_input_data")
    finalInputDF.show()
    
    ## Perform merge operation on incremental input data with MERGE INTO. This section of the code uses Spark SQL to showcase the expressive SQL approach of Iceberg to perform a Merge operation
    IcebergMergeOutputDF = spark.sql("""
    MERGE INTO job_catalog.human_resources.employee_details_iceberg t
    USING (SELECT Op, emp_no, name, department, city, salary FROM incremental_input_data) s
    ON t.emp_no = s.emp_no
    WHEN MATCHED AND s.Op = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET t.name = s.name, t.department = s.department, t.city = s.city, t.salary=s.salary
    WHEN NOT MATCHED THEN INSERT (emp_no, name, department, city, salary) VALUES (s.emp_no, s.name, s.department, s.city, s.salary)
    """)

    job.commit()
    

    
    
