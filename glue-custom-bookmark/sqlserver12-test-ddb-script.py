import sys, datetime, logging, traceback, json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Load DynamoDB service and define table       
dynamodb = boto3.resource('dynamodb')

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def update_attribute(dynamodb, table_name, query_key, query_value, attribute_name, attribute_value):
    table = dynamodb.Table(table_name)
    
    try:
        # get item
        response = table.get_item(Key={query_key: query_value})
        item = response['Item']
        
        # update
        item[attribute_name] = str(attribute_value)
        
        # put (idempotent)
        table.put_item(Item=item)
    except ClientError as err:
        logger.error(
            "Couldn't write item of %s into table %s. Here's why: %s: %s", item, table_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response

def query_table(dynamodb, table_name, query_key, query_value):
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key(query_key).eq(query_value)
        )
    except ClientError as err:
        logger.error(
            "Couldn't query for job status of %s. Here's why: %s: %s", query_value,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response
        
def get_max_value_dyf(input_dyf, col_name):
    df = input_dyf.toDF()
    max_value = df.agg({col_name: "max"}).collect()[0][0]
    return max_value
    
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

### SETUP JOB TABLE_ID OF DYNAMODB
ddb_table_id = 'sqlserver_data_dbo_test_col1'

### Get data source information from DynamoDB
ddb_bookmark = query_table(dynamodb, 'glue_bookmark', 'table_id', ddb_table_id)
print('ddb_bookmark', ddb_bookmark)

### Set parameter for data ingestion
ddb_bookmark_items = ddb_bookmark['Items'][0]
v_query = ''
v_table_name = ddb_bookmark_items['table_name']
v_connection = ddb_bookmark_items['connection_name']
v_bookmark_1_val = ddb_bookmark_items['bookmark_1_val']
v_bookmark_1_col = ddb_bookmark_items['bookmark_1_col']
v_bookmark_2_val = ddb_bookmark_items.get("bookmark_2_val", None) #optional
v_bookmark_2_col = ddb_bookmark_items.get("bookmark_2_col", None) #optional
v_where_clause = ddb_bookmark_items['where_clause']

### Assign bookmark where_clause for filtering
## Use {bookmark_1_val} and {bookmark_2_val} within where clause
where_clause = ''
if v_bookmark_2_val:
    where_clause = v_where_clause.format(bookmark_1_val = v_bookmark_1_val, bookmark_2_val = v_bookmark_2_val)
elif v_bookmark_1_val:
    where_clause = v_where_clause.format(bookmark_1_val = v_bookmark_1_val)
    
if where_clause:
    v_query = "select * from " + v_table_name + " where " + where_clause
else:
    v_query = "select * from " + v_table_name
    
print('v_query', v_query)

### START DATA INGESTION

Databasetable_node1 = glueContext.create_dynamic_frame.from_options(
    connection_type="custom.jdbc",
    connection_options={
        "query": v_query,
        "connectionName": v_connection,
    },
    transformation_ctx="Databasetable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Databasetable_node1,
    mappings=[
        ("val", "string", "val", "string"),
        ("id", "int", "id", "int"),
        ("ts", "timestamp", "ts", "timestamp"),
        ("col1", "int", "col1", "int"),
        ("str1", "string", "str1", "string")
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
    path="s3://glue-test-ap-southeast-1-130835040051/data/sql12_test/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=['job_ts'],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="glue_demo", catalogTableName="sql12_test"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(SQL_node1657534734141)

### END OF DATA INGESTION

### Check if data is not empty
if Databasetable_node1.count()>0:

    ### Get latest value and assign to bookmark 2 if required
    if v_bookmark_2_col:
        ### Get latest bookmark
        new_bookmark = get_max_value_dyf(Databasetable_node1, v_bookmark_2_col)
        print('new_bookmark_2', new_bookmark)
        ### Update bookmark in ddb
        update_attribute(dynamodb, 'glue_bookmark', 'table_id', ddb_table_id, 'bookmark_2_val', new_bookmark)
    
    ### Get latest value and assign to bookmark 1
    new_bookmark = get_max_value_dyf(Databasetable_node1, v_bookmark_1_col)
    print('new_bookmark_1', new_bookmark)
    update_attribute(dynamodb, 'glue_bookmark', 'table_id', ddb_table_id, 'bookmark_1_val', new_bookmark)





job.commit()
