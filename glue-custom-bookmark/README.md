# Glue Custom Bookmark

DynamoDB table:
- Table name: 'glue_bookmark'
- table_id (partition key): sqlserver_data_dbo_test
- bookmark_1_col : ts
- bookmark_1_val : 
- connection_name : sqlserver12-2
- db_name : data
- ingest : True
- table_name : dbo.test
- where_clause : ts>LEFT('{bookmark_1_val}', CHARINDEX('.', '{bookmark_1_val}') - 1)
