def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    ## @author Nico Anandito
    
    
    import psycopg2
    import boto3
    import json
    
    ## Step 1. Fetch the database connection details from AWS Secrets Manager
    secret_name = "<secret-name>"
    region_name = "ap-southeast-1"
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        print(f"Error fetching secret: {e}")
        raise e
    
    # Parse the secret value
    secret = json.loads(get_secret_value_response['SecretString'])
    db_host = secret['host']
    db_port = secret['port']
    db_name = secret['dbname']
    db_user = secret['username']
    db_password = secret['password']
    
    ## Step 2. Transform Dynamicframe into dataframe
    
    df = dfc.select(list(dfc.keys())[0]).toDF()
    
    ## Step 3. Create INSERT statement
    
    insert_statement = """
        INSERT INTO sample_table (id, name, value)
        VALUES ({new_id}, '{new_name}', '{new_value}')
        ON CONFLICT (id)
        DO UPDATE SET id = {new_id}, name = '{new_name}', value = '{new_value}'
    """
    
    ## Step 4. Create connection for each partitions, this will iterate INSERT for each row
    def save_partition(partition_iter):
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        for row in partition_iter:
            print(row)
            cursor.execute(insert_statement.format(new_id=row.id, new_name=row.name, new_value=row.value))
        conn.commit()
        cursor.close()
        conn.close()
    
    ## Step 5. Run save_partition function for each partition 
    df.foreachPartition(save_partition) 
    
    
    
