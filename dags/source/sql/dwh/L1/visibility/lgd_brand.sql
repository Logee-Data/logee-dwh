SELECT
  REPLACE(JSON_EXTRACT(data, '$.brandId'), '"', '') AS brand_id,
  REPLACE(JSON_EXTRACT(data, '$.companyId'), '"', '') AS company_id	,
  REPLACE(JSON_EXTRACT(data, '$.brandName'), '"', '') AS brand_name,
  JSON_EXTRACT_STRING_ARRAY(data, '$.categoryIds') AS category_ids,
  CAST(REPLACE(JSON_EXTRACT(data, '$.isActive'), '"', '') AS BOOL) AS is_active,
  CAST(REPLACE(JSON_EXTRACT(data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.insert_date_dma'), '"', '') AS TIMESTAMP) AS insert_date_dma,
  ts AS published_timestamp
FROM
  `logee-data-prod.logee_datalake_raw_production.visibility_lgd_brand` 
WHERE
  _date_partition IN ('{{ ds }}', '{{ next_ds }}')
  AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'