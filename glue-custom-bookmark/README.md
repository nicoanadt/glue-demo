# Glue Custom Bookmark

### How to use

1. Create DynamoDB table `glue_bookmark` and setup an table entry
2. Set glue job variable  `ddb_table_id` with the `table_id` of DynamoDB
3. Set table schema in glue job (in `ApplyMapping`)
4. Set target in glue job:
    - `path` : s3 target
    - `catalogDatabase` : target glue database
    - `catalogTableName` : target glue table

### DynamoDB table:
- Table name: `glue_bookmark`

| Key | Example Value | Description |
| --- | --- | --- |
| table_id | `sqlserver_data_dbo_test` | Partition key. This is a unique value for a particular table |
| ingest | True | Boolean type. True/False. If True then ingest |
| connection_name | `sqlserver12-2` | Connection name for custom JDBC connection in Glue. (Connection, not connector) |
| db_name | `data_db` | Database name in source database |
| table_name | `dbo.items` | Table name in source database |
| where_clause | `item_id>'{bookmark_1_val}'`<br><br>or<br><br>`item_id>'{bookmark_1_val}' and updated_ts>'{bookmark_2_val}'` | Where clause as filter query for bookmark. Use {bookmark\_1\_val} and {bookmark\_2\_val} respectively when required. Use quote `'` for string value. |
| bookmark\_1\_col | `item_id` | Column name for bookmark. |
| bookmark\_1\_val | \[empty\] | Keep empty for initial load. Will be updated in each run |
| bookmark\_2\_col | `updated_ts` | Column name for bookmark.<br>OPTIONAL. Create only when required. |
| bookmark\_2\_val | \[empty\] | OPTIONAL. Create only when required. Keep empty for initial load. Will be updated in each run |


### Note:

- For timestamp column, adjust the `where_clause` as required based value in the `bookmark_val`. 
    - For example, in SQL Server the `bookmark_val` format is `2022-07-21 22:29:43.50000` while they only accept datetime format like this `2022-07-21 22:29:43.500`. To resolve this we need to trim the last 2 character using the following `where_clause`:
        ```
        ts>LEFT('{bookmark_1_val}', CHARINDEX('.', '{bookmark_1_val}') + 2)
        ```
    - Another example in Postgres we only need the following `where_clause` for the same timestamp string `2022-07-21 22:29:43.50000`:
        ```
        ts>to_timestamp('{bookmark_1_val}', 'YYYY-MM-DD HH24:MI:SS.US')
        ```
