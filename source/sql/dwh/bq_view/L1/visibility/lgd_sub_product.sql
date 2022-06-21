WITH 

base AS (
  SELECT * FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_sub_product`
  WHERE _date_partition >= '2022-01-01'
)

-- BEGIN SUB PRODUCT VARIANT

,pre_sub_product_variant AS (
  SELECT 
    data,
    ts AS published_timestamp,
    sub_product_variant
  FROM
    BASE,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(data, '$.subProductVariant'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS sub_product_variant
  WHERE
    _date_partition >= '2022-01-01'
)

,sub_product_variant AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG(
      STRUCT(
        IF(JSON_EXTRACT_SCALAR(sub_product_variant, '$.variantName') = "", NULL, JSON_EXTRACT_SCALAR(sub_product_variant, '$.variantName')) AS variant_name,
        IF(JSON_EXTRACT_SCALAR(sub_product_variant, '$.variant') = "", NULL, JSON_EXTRACT_SCALAR(sub_product_variant, '$.variant')) AS variant
      )
    ) AS sub_product_variant
  FROM
    pre_sub_product_variant
  GROUP BY 1,2
)

-- END SUB PRODUCT VARIANT

-- BEGIN BOOKED STOCK

,pre_booked_stock AS (
  SELECT 
    data,
    ts AS published_timestamp,
    booked_stock
  FROM
    BASE,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(data, '$.bookedStock'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS booked_stock
  WHERE
    _date_partition >= '2022-01-01'
)

,booked_stock AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG(
      STRUCT(
        IF(JSON_EXTRACT_SCALAR(booked_stock, '$.orderId') = "", NULL, JSON_EXTRACT_SCALAR(booked_stock, '$.orderId')) AS order_id,
        IF(JSON_EXTRACT_SCALAR(booked_stock, '$.stock') = "", NULL, JSON_EXTRACT_SCALAR(booked_stock, '$.stock')) AS stock
      )
    ) AS booked_stock
  FROM
    pre_booked_stock
  GROUP BY 1,2
)

-- END BOOKED STOCK

SELECT
  IF(REPLACE(JSON_EXTRACT(A.data, '$.subProductId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.subProductId'), '"', '')) AS subProductId,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.companyId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.companyId'), '"', '')) AS company_id,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.brandId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.brandId'), '"', '')) AS brand_id,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.categoryId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.categoryId'), '"', '')) AS category_id,
  REPLACE(JSON_EXTRACT(A.data, '$.productId'), '"', '') AS product_id,
  REPLACE(JSON_EXTRACT(A.data, '$.subProductName'), '"', '') AS sub_product_name,
  REPLACE(JSON_EXTRACT(A.data, '$.subProductSize'), '"', '') AS sub_product_size,
  JSON_EXTRACT_ARRAY(A.data, '$.subProductColors' AS sub_product_colors,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.subProductStock'), '"', '') AS INT64) AS sub_product_stock,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.subProductDiscountPercent'), '"', '') AS FLOAT64) AS sub_product_discount_percent,
  REPLACE(JSON_EXTRACT(A.data, '$.subProductDescription'), '"', '') AS sub_product_description,
  REPLACE(JSON_EXTRACT(A.data, '$.subProductUnit'), '"', '') AS sub_product_unit,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.subProductPrice'), '"', '') AS INT64) AS sub_product_price,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.subProductWeight'), '"', '') AS INT64) AS sub_product_weight,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.externalId'), '"', '')) AS external_id,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.onShelf'), '"', '') AS BOOL) AS on_shelf,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.productOnShelf'), '"', '') AS BOOL) AS product_on_shelf,
  B.sub_product_variant AS sub_product_variant,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.subProductsSize'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.subProductsSize'), '"', '')) AS sub_products_size,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.subProductMinimumOrder'), '"', '') AS INT64) AS sub_product_minimum_order,
  C.booked_stock AS booked_stock,
  STRUCT (
    IF(REPLACE(JSON_EXTRACT(A.data, '$.categoryIds.categoryId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.categoryIds.categoryId'), '"', '')) AS category_id,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.categoryIds.subCategoryId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.categoryIds.subCategoryId'), '"', '')) AS sub_category_id,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.categoryIds.subSubCategoryId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.categoryIds.subCategoryId'), '"', '')) AS sub_sub_category_id
  ) AS category_ids,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isTax'), '"', '') AS BOOL) AS is_tax,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isBonus'), '"', '') AS BOOL) AS is_bonus,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '')) AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.insert_date_dma'), '"', '') AS TIMESTAMP) AS insert_date_dma,
  A.data AS original_data,
  A.ts AS published_timestamp
FROM
  base A

  LEFT JOIN sub_product_variant B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN booked_stock C
  ON A.data = C.data
  AND A.ts = C.published_timestamp