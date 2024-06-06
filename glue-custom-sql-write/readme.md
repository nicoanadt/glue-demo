# Glue Custom SQL Write

This custom transform script is created to write custom SQL into the target Postgres database.

The script will not use the spark dataframe writer to write into the target database. Instead it will use the python `psycopg2` postgre library to connect to database. The function is called using `foreachpartition` to execute INSERT statement for each rows, using a database connection for each partition.

Note: Performance wise, using this method will not be as optimized as dataframe writer, thus ensure to monitor the write performance based on your use case. Adjust `repartition` as needed.

### Prerequisites
- Set job parameter as:
  - `--additional-python-modules` : `psycopg2-binary==2.9.9`
 
## How to use

1. Create a custom transform in Glue Visual ETL. This screenshot is a sample where the custom transform is used in visual ETL as the final node. 

![Glue job sample](https://raw.githubusercontent.com/nicoanadt/glue-demo/main/glue-custom-sql-write/img/DKF%20Glue%201.png)

2. The custom transform python code is attached as `custom-transform.py`. Copy the code to the Glue node, and make the necessary changes:
  - Change the schema in the INSERT statement
  - Change the schema in the cursor.execute statement
  - Change the secret manager name
  - Verify the INSERT statement 

![Glue job detail](https://raw.githubusercontent.com/nicoanadt/glue-demo/main/glue-custom-sql-write/img/DKF%20Glue%202.png) 
 
