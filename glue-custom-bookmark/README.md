# Glue Custom Bookmark

DynamoDB table:
- Table name: `glue_bookmark`

|  Key   |  Example Value    |   Description  |
| --- | --- | --- |
| table_id | `sqlserver_data_dbo_test` | Partition key. This is a unique value for a particular table |
| ingest | True | Boolean type. True/False. If True then ingest |
| connection_name | `sqlserver12-2` | Connection name for custom JDBC connection in Glue. (Connection, not connector) |
| db_name | `data_db` | Database name in source database |
| table_name | `dbo.items` | Table name in source database |
| where_clause | `item_id>'{bookmark_1_val}'` | Where clause as filter query for bookmark. Use {bookmark\_1\_val} and {bookmark\_2\_val} respectively when required. |
| bookmark\_1\_col | `item_id` | Column name for bookmark. |
| bookmark\_1\_val | \[empty\] | Keep empty for initial load. Will be updated in each run |
| bookmark\_2\_col | `updated_ts` | Column name for bookmark.<br><br>OPTIONAL. Create only when required. |
| bookmark\_2\_val | \[empty\] | OPTIONAL. Create only when required. Keep empty for initial load. Will be updated in each run |
