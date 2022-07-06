WITH base AS (
  SELECT * except(data), data AS original_data
  FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_product`
  WHERE 
    _date_partition IN ('{{ ds }}', '{{ next_ds }}')
    AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- BEGIN CLEANING DATA

-- END CLANING DATA

SELECT
  json_extract_scalar(original_data, '$.productId') AS product_id,
  json_extract_scalar(original_data, '$.companyId') AS company_id,
  json_extract_scalar(original_data, '$.productName') AS product_name,
  STRUCT(
    CAST(JSON_EXTRACT_SCALAR(json_extract_scalar(original_data, '$.productDimension'), '$.length') AS FLOAT64) AS length,
    CAST(JSON_EXTRACT_SCALAR(json_extract_scalar(original_data, '$.productDimension'), '$.width') AS FLOAT64) AS width,
    CAST(JSON_EXTRACT_SCALAR(json_extract_scalar(original_data, '$.productDimension'), '$.height') AS FLOAT64) AS height
  ) AS product_dimension,
  STRUCT(
    JSON_EXTRACT_SCALAR(json_extract_scalar(original_data, '$.categoryIds'), '$.categoryId') AS category_id,
    JSON_EXTRACT_SCALAR(json_extract_scalar(original_data, '$.categoryIds'), '$.subCategoryId') AS sub_category_id,
    JSON_EXTRACT_SCALAR(json_extract_scalar(original_data, '$.categoryIds'), '$.subSubCategoryId') AS sub_sub_category_id
  ) AS category_ids,
  IF(json_extract_scalar(original_data, '$.brandId') = '', NULL, json_extract_scalar(original_data, '$.brandId')) AS brand_id,
  CAST(json_extract_scalar(original_data, '$.onShelf') AS BOOL) AS on_shelf,
  IF(json_extract_scalar(original_data, '$.externalId') = '', NULL, json_extract_scalar(original_data, '$.externalId')) AS external_id,
  IF(json_extract_scalar(original_data, '$.productSpesification') = '', NULL, json_extract_scalar(original_data, '$.productSpesification')) AS product_spesification,
  json_extract_scalar(original_data, '$.productDescription') AS product_description,
  json_extract_scalar(original_data, '$.productUnit') AS product_unit,
  CAST(json_extract_scalar(original_data, '$.productWeight') AS FLOAT64) AS product_weight,
  CAST(json_extract_scalar(original_data, '$.productPrice') AS FLOAT64) AS product_price,
  CAST(json_extract_scalar(original_data, '$.productStock') AS INT64) AS product_stock,
  CAST(json_extract_scalar(original_data, '$.productMinimumOrder') AS INT64) AS product_minimum_order,
  JSON_EXTRACT_ARRAY(original_data, '$.productVariant') AS product_variant,
  CAST(json_extract_scalar(original_data, '$.isTax') AS BOOL) AS is_tax,
  CAST(json_extract_scalar(original_data, '$.isBonus') AS BOOL) AS is_bonus,
  CAST(json_extract_scalar(original_data, '$.isDeleted') AS BOOL) AS is_deleted,
  CAST(json_extract_scalar(original_data, '$.createdAt') AS TIMESTAMP) AS created_at,
  json_extract_scalar(original_data, '$.createdBy') AS created_by,
  CAST(json_extract_scalar(original_data, '$.modifiedAt') AS TIMESTAMP) AS modified_at,
  json_extract_scalar(original_data, '$.modifiedBy') AS modified_by,
  CAST(json_extract_scalar(original_data, '$.insert_date_dma') AS TIMESTAMP) AS insert_date_dma,
  ts AS published_timestamp
FROM base
