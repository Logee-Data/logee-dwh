-- CHECK

WITH check AS (

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_id' AS column,
      IF(product_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_id IS NULL or product_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'company_id' AS column,
      IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE company_id IS NULL or company_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_name' AS column,
      IF(product_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_name IS NULL or product_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_length' AS column,
      IF(product_length IS NULL, 'Column can not be NULL', IF(product_length = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_length IS NULL or product_length <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_height' AS column,
      IF(product_height IS NULL, 'Column can not be NULL', IF(product_height = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_height IS NULL or product_height <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_width' AS column,
      IF(product_width IS NULL, 'Column can not be NULL', IF(product_width = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_width IS NULL or product_width <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_category_id' AS column,
      IF(product_category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_category_id IS NULL or product_category_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_sub_category_id' AS column,
      IF(product_sub_category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_sub_category_id IS NULL or product_sub_category_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_sub_sub_category_id' AS column,
      IF(product_sub_sub_category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_sub_sub_category_id IS NULL or product_sub_sub_category_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'brand_id' AS column,
      IF(brand_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE brand_id IS NULL or brand_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_description' AS column,
      IF(product_description IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_description IS NULL or product_description = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_weight' AS column,
      IF(product_weight IS NULL, 'Column can not be NULL', IF(product_weight = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_weight IS NULL or product_weight <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_price' AS column,
      IF(product_price IS NULL, 'Column can not be NULL', IF(product_price = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_price IS NULL or product_price <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_stock' AS column,
      IF(product_stock IS NULL, 'Column can not be NULL', 'Column can not be a negative number') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_stock IS NULL or product_stock < 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'product_minimum_order' AS column,
      IF(product_minimum_order IS NULL, 'Column can not be NULL', 'Column can not be a negative number') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product`
  WHERE product_minimum_order IS NULL or product_minimum_order < 0

)

,aggregated_check AS (
  SELECT
    original_data,
    published_timestamp,
    ARRAY_AGG(
      quality_check
    ) AS quality_check
  FROM check
  GROUP BY 1, 2
)

SELECT
  A.*,
  B.quality_check
FROM
  `logee-data-prod.L1_visibility.lgd_product` A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp