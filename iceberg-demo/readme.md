# Iceberg Demo for Glue

This is a repository created to showcase Iceberg capability in Glue for demo purposes. Disclaimer: The codes are provided as-is and no support is provided

## 1. Glue job for Iceberg

This glue is to transform data from Raw layer to Transformed layer

Parameters:
- Glue 4.0
- Bookmark: `enabled`
- Enable Auto-scaling to save cost
- Use lowest number of workers during testing to save cost
- Job parameters:
  - `--datalake-formats` : `iceberg`
  - `--iceberg_job_catalog_warehouse` : 's3://<s3_bucket>/<path>'


## 2. Athena Iceberg table for Transformed layer

Sample DDL for target table

```
CREATE TABLE human_resources.employee_details_iceberg (
  emp_no string,
  name string,
  department string,
  city string,
  salary string)
LOCATION 's3://<s3_bucket>/<path>'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='parquet'
);
```
