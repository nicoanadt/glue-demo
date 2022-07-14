CREATE TABLE iceberg_demo.sql12_items_transformed (
  item_id string,
  style string,
  updated_date timestamp,
  category string,
  job_ts string) 
PARTITIONED BY (category) 
LOCATION 's3://glue-iceberg-demo-130835040051-apsoutheast1/iceberg-output/' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_target_data_file_size_bytes'='536870912' 
)
