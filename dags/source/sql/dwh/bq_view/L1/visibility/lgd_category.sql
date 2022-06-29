SELECT
  IF(REPLACE(JSON_EXTRACT(data, '$.categoryId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.categoryId'), '"', ''))  AS category_id,
  REPLACE(JSON_EXTRACT(data, '$.categoryName'), '"', '') AS category_name,
  IF(REPLACE(JSON_EXTRACT(data, '$.categoryImage'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.categoryImage'), '"', ''))  AS category_image,
  REPLACE(JSON_EXTRACT(data, '$.categoryType'), '"', '') AS category_type,
  CAST(REPLACE(JSON_EXTRACT(data, '$.totalProductBind'), '"', '') AS INT64) AS total_product_bind,
  CAST(REPLACE(JSON_EXTRACT(data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  IF(REPLACE(JSON_EXTRACT(data, '$.parentId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.parentId'), '"', ''))  AS parent_id,
  data AS original_data,
  ts AS published_timestamp
FROM
  `logee-data-prod.logee_datalake_raw_production.visibility_lgd_category` 
WHERE
  _date_partition >= "2022-01-01"