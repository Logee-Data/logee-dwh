SELECT  
  REPLACE(JSON_EXTRACT(data, '$.salesId'),'"','') AS sales_id,
  REPLACE(JSON_EXTRACT(data, '$.salesName'),'"','') AS sales_name,
  REPLACE(JSON_EXTRACT(data, '$.salesCode'),'"','') AS sales_code,
  REPLACE(JSON_EXTRACT(data, '$.salesType'),'"','') AS sales_type,
  REPLACE(JSON_EXTRACT(data, '$.userId'),'"','') AS user_id,
  REPLACE(JSON_EXTRACT(data, '$.username'),'"','') AS username,
  JSON_EXTRACT(data, '$.userType') AS user_type,
  REPLACE(JSON_EXTRACT(data, '$.email'),'"','') AS email,
  REPLACE(JSON_EXTRACT(data, '$.phoneNumber'),'"','') AS phone_number,
  REPLACE(JSON_EXTRACT(data, '$.companyId'),'"','') AS company_id,
  JSON_EXTRACT(data, '$.apps') AS apps,
  REPLACE(JSON_EXTRACT(data, '$.employeeStatus'),'"','') AS employee_status,
  REPLACE(JSON_EXTRACT(data, '$.status'),'"','') AS status,
  CAST(JSON_EXTRACT(data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  CAST(JSON_EXTRACT(data, '$.isBind') AS BOOLEAN) AS is_bind,
  CAST(REPLACE(JSON_EXTRACT(data, '$.createdAt'),'"','') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(data, '$.createdBy'),'"','') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.modifiedAt'),'"','') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(data, '$.modifiedBy'),'"','') AS modified_by,
  ts AS published_timestamp
FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_sales` 
WHERE 
  _date_partition IN ('{{ ds }}', '{{ next_ds }}')
  AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'