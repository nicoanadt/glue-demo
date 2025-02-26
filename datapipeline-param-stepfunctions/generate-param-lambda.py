import datetime
import uuid

def lambda_handler(event, context):
    # Get current timestamp in YYYYMMDD_HHMMSS format
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Generate a short unique identifier (first 6 characters of UUID)
    unique_id = str(uuid.uuid4())[:6]
    
    # Combine timestamp and unique id
    job_id = f"JOB_{timestamp}_{unique_id}"
    
    return {
        'statusCode': 200,
        'job_id': job_id
    }
