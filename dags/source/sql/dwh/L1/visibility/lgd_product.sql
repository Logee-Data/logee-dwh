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
  ts AS published_timestamp
  FROM
    BASE
)

-- END CLANING DATA

-- BEGIN PRODUCT VARIANT

, product_variant_1 AS (
  SELECT 
    data,
    ts AS published_timestamp,
    product_variant
  FROM
    BASE,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(data, '$.productVariant'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS product_variant
)

,product_variant_2 AS (
  SELECT
    data,
    published_timestamp,
    JSON_EXTRACT_SCALAR(product_variant,  '$.variantName')AS variant_name,
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
    data,
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
  JSON_EXTRACT_SCALAR(B.data, '$.productId') AS product_id,
  JSON_EXTRACT_SCALAR(B.data, '$.companyId') AS company_id,
  JSON_EXTRACT_SCALAR(B.data, '$.productName') AS product_name,
  STRUCT(
    CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(B.data, '$.productDimension'), '$.length') AS FLOAT64) AS length,
    CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(B.data, '$.productDimension'), '$.width') AS FLOAT64) AS width,
    CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(B.data, '$.productDimension'), '$.height') AS FLOAT64) AS height
  ) AS product_dimension,
  STRUCT(
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(B.data, '$.categoryIds'), '$.categoryId') AS category_id,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(B.data, '$.categoryIds'), '$.subCategoryId') AS sub_category_id,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(B.data, '$.categoryIds'), '$.subSubCategoryId') AS sub_sub_category_id
  ) AS category_ids,
  IF(JSON_EXTRACT_SCALAR(B.data, '$.brandId') = '', NULL, JSON_EXTRACT_SCALAR(B.data, '$.brandId')) AS brand_id,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.onShelf') AS BOOL) AS on_shelf,
  IF(JSON_EXTRACT_SCALAR(B.data, '$.externalId') = '', NULL, JSON_EXTRACT_SCALAR(B.data, '$.externalId')) AS external_id,
  IF(JSON_EXTRACT_SCALAR(B.data, '$.productSpesification') = '', NULL, JSON_EXTRACT_SCALAR(B.data, '$.productSpesification')) AS product_spesification,
  IF(JSON_EXTRACT_SCALAR(B.data, '$.productDescription') = ' ', NULL, JSON_EXTRACT_SCALAR(B.data, '$.productDescription')) AS product_description,
  JSON_EXTRACT_SCALAR(B.data, '$.productUnit') AS product_unit,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.productWeight') AS FLOAT64) AS product_weight,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.productPrice') AS FLOAT64) AS product_price,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.productStock') AS INT64) AS product_stock,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.productMinimumOrder') AS INT64) AS product_minimum_order,
  A.product_variant, 
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.isTax') AS BOOL) AS is_tax,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.isBonus') AS BOOL) AS is_bonus,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.isDeleted') AS BOOL) AS is_deleted,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.createdAt') AS TIMESTAMP) AS created_at,
  JSON_EXTRACT_SCALAR(B.data, '$.createdBy') AS created_by,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.modifiedAt') AS TIMESTAMP) AS modified_at,
  JSON_EXTRACT_SCALAR(B.data, '$.modifiedBy') AS modified_by,
  CAST(JSON_EXTRACT_SCALAR(B.data, '$.insert_date_dma') AS TIMESTAMP) AS insert_date_dma,
  B.published_timestamp
FROM product_variant A
  FULL OUTER JOIN pre_data B
  ON A.data = B.data
  AND A.published_timestamp = B.published_timestamp