WITH base AS (
  SELECT *
  FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_product`
  WHERE 
    _date_partition IN ('{{ ds }}', '{{ next_ds }}')
    AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- BEGIN CLEANING DATA

, pre_data AS (
  SELECT
    * REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(data, '\\', ''),
        ':"{',':{'),
      '}",',
        '},'
      ) AS data
    ),
  data AS original_data,
  ts AS published_timestamp
  FROM
    BASE
)

-- END CLANING DATA

-- BEGIN PRODUCT VARIANT

, product_variant_1 AS (
  SELECT 
    data AS original_data,
    ts AS published_timestamp,
    product_variant
  FROM
    BASE,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(data, '$.productVariant'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS product_variant
  WHERE
    _date_partition >= '2022-01-01'
)

,product_variant_2 AS (
  SELECT
    original_data,
    published_timestamp,
    JSON_EXTRACT(product_variant,  '$.variantName')AS variant_name,
    ARRAY_AGG(
      REPLACE(variant_list, '"', '')
    ) AS variant_list
  FROM
    product_variant_1,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(product_variant, '$.variantList'), '$.')) AS variant_list
  GROUP BY 1,2,3
)

, product_variant AS(
  SELECT 
    original_data,
    published_timestamp,
    ARRAY_AGG(
    STRUCT(
      variant_name,
      variant_list
    )
  ) AS product_variant
FROM product_variant_2
GROUP BY 1,2
)

-- END PRODUCT VARIANT

SELECT 
  REPLACE(JSON_EXTRACT(B.data, '$.productId'), '"', '') AS product_id,
  REPLACE(JSON_EXTRACT(B.data, '$.companyId'), '"', '') AS company_id,
  REPLACE(JSON_EXTRACT(B.data, '$.productName'), '"', '') AS product_name,
  CAST(JSON_EXTRACT(B.data, '$.productDimension.length') AS FLOAT64) AS product_length,
  CAST(JSON_EXTRACT(B.data, '$.productDimension.height') AS FLOAT64) AS product_height,
  CAST(JSON_EXTRACT(B.data, '$.productDimension.width') AS FLOAT64) AS product_width,
  REPLACE(JSON_EXTRACT(B.data, '$.categoryIds.categoryId'), '"', '') AS product_category_id,
  REPLACE(JSON_EXTRACT(B.data, '$.categoryIds.subCategoryId'), '"', '') AS product_sub_category_id,
  REPLACE(JSON_EXTRACT(B.data, '$.categoryIds.subSubCategoryId'), '"', '') AS product_sub_sub_category_id,
  IF(REPLACE(JSON_EXTRACT(B.data, '$.brandId'), '"', '')="", NULL, REPLACE(JSON_EXTRACT(data, '$.brandId'), '"', '')) AS brand_id,
  CAST(JSON_EXTRACT(B.data, '$.onShelf') AS BOOLEAN) AS is_on_shelf,
  IF(REPLACE(JSON_EXTRACT(B.data, '$.externalId'),'"','')="", NULL, REPLACE(JSON_EXTRACT(data, '$.externalId'),'"','')) AS external_id,
  IF(REPLACE(JSON_EXTRACT(B.data, '$.productSpesification'),'"','')="", NULL, REPLACE(JSON_EXTRACT(data, '$.productSpesification'),'"','')) AS product_spesification,
  REPLACE(JSON_EXTRACT(B.data, '$.productDescription'), '"', '') AS product_description,
  REPLACE(JSON_EXTRACT(B.data, '$.productUnit'),'"','') AS product_unit,
  CAST(JSON_EXTRACT(B.data, '$.productWeight') AS FLOAT64) AS product_weight,
  CAST(JSON_EXTRACT(B.data, '$.productPrice') AS FLOAT64) AS product_price,
  CAST(JSON_EXTRACT(B.data, '$.productStock') AS INT64) AS product_stock,
  CAST(JSON_EXTRACT(B.data, '$.productMinimumOrder') AS INT64) AS product_minimum_order,
A.product_variant, 
CAST(JSON_EXTRACT(B.original_data, '$.isTax') AS BOOLEAN) AS is_tax,
CAST(JSON_EXTRACT(B.original_data, '$.isBonus') AS BOOLEAN) AS is_bonus,
CAST(JSON_EXTRACT(B.original_data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
REPLACE(JSON_EXTRACT(B.original_data, '$.createdBy'),'"','') AS created_by,
CAST(REPLACE(JSON_EXTRACT(B.original_data, '$.createdAt'),'"','') AS TIMESTAMP) AS created_at,
REPLACE(JSON_EXTRACT(B.original_data, '$.modifiedBy'),'"','') AS modified_by,
CAST(REPLACE(JSON_EXTRACT(B.original_data, '$.modifiedAt'),'"','') AS TIMESTAMP) AS modified_at,
B.published_timestamp
FROM product_variant A
  FULL OUTER JOIN pre_data B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp
