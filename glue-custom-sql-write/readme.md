# Glue Custom SQL Write

This custom transform script is created to write custom SQL into the target Postgres database.

The script will not use the spark dataframe writer to write into the target database. Instead it will use the python `psycopg2` postgre library to connect to database. The function is called using `foreachpartition` to execute INSERT statement for each rows, using a database connection for each partition.

Note: Performance wise, using this method will not be as optimized as dataframe writer, thus ensure to monitor the write performance based on your use case. Adjust `repartition` as needed.

### Prerequisites
- Set job parameter as:
  - `--additional-python-modules` : `psycopg2-binary==2.9.9`
 
