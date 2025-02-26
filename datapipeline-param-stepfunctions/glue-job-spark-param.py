import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Define the parameters we want to pass to the job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'job_id'])

# Initialize the Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize the job with the job name from parameters
job.init(args['JOB_NAME'], args)

# Get the job_id parameter value
job_id = args['job_id']

# Print the received job_id (for demonstration)
print(f"Received job_id: {job_id}")

# Your ETL logic here
# Example: You can use the job_id in your transformations
# dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
#     database="your_database",
#     table_name="your_table",
#     push_down_predicate=f"job_id='{job_id}'"
# )

# Commit the job
job.commit()
