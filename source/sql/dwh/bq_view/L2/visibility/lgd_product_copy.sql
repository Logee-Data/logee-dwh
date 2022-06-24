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
      IF(product_length IS NULL, 'Column can not be NULL', IF(product_length = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_length IS NULL or product_length <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_height' AS column,
      IF(product_height IS NULL, 'Column can not be NULL', IF(product_height = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_height IS NULL or product_height <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_width' AS column,
      IF(product_width IS NULL, 'Column can not be NULL', IF(product_width = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
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
      'external_id' AS column,
      IF(external_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE external_id IS NULL or external_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_spesification' AS column,
      IF(product_spesification IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_spesification IS NULL or product_spesification = ''

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
      'product_unit' AS column,
      IF(product_unit IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_unit IS NULL or product_unit = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_weight' AS column,
      IF(product_weight IS NULL, 'Column can not be NULL', IF(product_weight = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_weight IS NULL or product_weight <= 0 

   UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_price' AS column,
      IF(product_price IS NULL, 'Column can not be NULL', IF(product_price = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_price IS NULL or product_price <= 0 

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_minimum_order' AS column,
      IF(product_minimum_order IS NULL, 'Column can not be NULL', IF(product_minimum_order = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE product_minimum_order IS NULL or product_minimum_order <= 0 

-- product_variant  (struct)

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_stock' AS column,
      "Contains zero product_stock when value of is_on_shelf is TRUE" AS notes
    ) quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE
    product_stock = 0 AND is_on_shelf = TRUE

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_on_shelf' AS column,
      "is_on_shelf is TRUE when value of product_stock equals zero" AS notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE
    product_stock = 0 AND is_on_shelf = TRUE

  UNION ALL

    SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'product_stock' AS column,
      "Contains a number of product_stock when value of is_on_shelf is FALSE" AS notes
    ) quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE
    product_stock > 0 AND is_on_shelf = FALSE

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_on_shelf' AS column,
      "is_on_shelf is FALSE when value of product_stock equals non zero value" AS notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE
    product_stock > 0 AND is_on_shelf = FALSE

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_tax' AS column,
      IF(is_tax IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE is_tax IS NULL 

    UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_bonus' AS column,
      IF(is_bonus IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE is_bonus IS NULL 

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_deleted' AS column,
      IF(is_deleted IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_product` 
  WHERE is_deleted IS NULL 

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
  `logee-data-prod.L1_visibility.lgd_product`  A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp