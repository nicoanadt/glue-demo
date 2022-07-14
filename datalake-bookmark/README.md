# Datalake Bookmark Demo


Author @nicanand

## 1. Data source to Raw Glue job
- Job name: `sqlserver12-items-native-bookmark-script.py`
- Bookmark is enabled

## 2. Raw to transformed Glue Job Config
- Job name: `sqlserver12-items-iceberg-transformed.py`
- Bookmark is enabled
- Import Iceberg Connector for Glue 3.0 connector from marketplace
- Add Glue job parameter: 
  - `Key`: `--iceberg_job_catalog_warehouse`
  - `Value`: `s3://[iceberg S3 location]/data/iceberg/`

