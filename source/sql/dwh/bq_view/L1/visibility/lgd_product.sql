-- existing query
WITH base AS (
  SELECT
    * REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(data, '\\', ''),
          ':"{',
          ':{'
        ),
        '}",',
        '},'
      ) AS data
    )
  FROM
    `logee-data-prod.logee_datalake_raw_production.visibility_lgd_product`
  WHERE
    _date_partition >= '2022-01-01'
)

SELECT
  REPLACE(JSON_EXTRACT(data, '$.productId'), '"', '') AS product_id,
  REPLACE(JSON_EXTRACT(data, '$.companyId'), '"', '') AS company_id,
  REPLACE(JSON_EXTRACT(data, '$.productName'), '"', '') AS product_name,
  CAST(JSON_EXTRACT(data, '$.productDimension.length') AS FLOAT64) AS product_length,
  CAST(JSON_EXTRACT(data, '$.productDimension.height') AS FLOAT64) AS product_height,
  CAST(JSON_EXTRACT(data, '$.productDimension.width') AS FLOAT64) AS product_width,
  REPLACE(JSON_EXTRACT(data, '$.categoryIds.categoryId'), '"', '') AS product_category_id,
  REPLACE(JSON_EXTRACT(data, '$.categoryIds.subCategoryId'), '"', '') AS product_sub_category_id,
  REPLACE(JSON_EXTRACT(data, '$.categoryIds.subSubCategoryId'), '"', '') AS product_sub_sub_category_id,
  IF(REPLACE(JSON_EXTRACT(data, '$.brandId'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.brandId'), '"', '')) AS brand_id,
  CAST(JSON_EXTRACT(data, '$.onShelf') AS BOOLEAN) AS is_on_shelf,
  CAST(JSON_EXTRACT(data, '$.isTax') AS BOOLEAN) AS is_tax,
  CAST(JSON_EXTRACT(data, '$.isBonus') AS BOOLEAN) AS is_bonus,
  CAST(JSON_EXTRACT(data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  IF(REPLACE(JSON_EXTRACT(data, '$.productDescription'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.productDescription'), '"', '')) AS product_description,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.productWeight'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.productWeight'), '"', '')) AS FLOAT64) AS product_weight,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.productPrice'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.productPrice'), '"', '')) AS FLOAT64) AS product_price,
  CAST(JSON_EXTRACT(data, '$.productStock') AS INT64) AS product_stock,
  CAST(JSON_EXTRACT(data, '$.productMinimumOrder') AS INT64) AS product_minimum_order,
  IF(REPLACE(JSON_EXTRACT(data, '$.externalId'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.externalId'), '"', '')) AS external_id,
  IF(REPLACE(JSON_EXTRACT(data, '$.productSpesification'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.productSpesification'), '"', '')) AS product_specification,
  IF(REPLACE(JSON_EXTRACT(data, '$.productUnit'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.productUnit'), '"', '')) AS product_unit,
  IF(REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '')) AS created_by,
  IF(REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '')) AS modified_by,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '')) AS TIMESTAMP) AS created_at,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '')) AS TIMESTAMP) AS modified_at,
  data AS original_data,
  ts AS published_timestamp
FROM base