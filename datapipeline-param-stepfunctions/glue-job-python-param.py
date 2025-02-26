import sys
import boto3
from awsglue.utils import getResolvedOptions
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_job_parameters():
    """Get job parameters passed to the Glue job"""
    try:
        args = getResolvedOptions(sys.argv, ['job_id'])
        return args
    except Exception as e:
        logger.error(f"Error getting job parameters: {str(e)}")
        raise e



def main():
    try:
        # Get job parameters
        args = get_job_parameters()
        job_id = args['job_id']
        
        logger.info(f"Starting job with job_id: {job_id}")
        
        # Process the data
        
        
        logger.info(f"Job completed successfully for job_id: {job_id}")
        return
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
