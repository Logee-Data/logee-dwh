SELECT
  REPLACE(JSON_EXTRACT(data, '$.brandId'), '"', '') AS brand_id,
  REPLACE(JSON_EXTRACT(data, '$.brandName'), '"', '') AS brand_name,
  JSON_EXTRACT_STRING_ARRAY(data, '$.categoryIds') AS category_ids,
  CAST(REPLACE(JSON_EXTRACT(data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  data,
  ts AS published_timestamp
FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_brand` 
WHERE _date_partition >= "2022-01-01"