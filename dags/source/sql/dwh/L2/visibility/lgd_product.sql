WITH base AS (
  SELECT 
    *
  FROM 
    `logee-data-prod.L1_visibility.lgd_product`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)


-- CHECK 

-- , check AS (

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_id' AS column,
--       IF(product_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base 
--   WHERE product_id IS NULL or product_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'company_id' AS column,
--       IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base 
--   WHERE company_id IS NULL or company_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_name' AS column,
--       IF(product_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_name IS NULL or product_name = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_length' AS column,
--       IF(product_dimension.length IS NULL, 'Column can not be NULL', IF(product_dimension.length = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_dimension.length IS NULL or product_dimension.length <= 0

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_height' AS column,
--       IF(product_dimension.height IS NULL, 'Column can not be NULL', IF(product_dimension.height = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_dimension.height IS NULL or product_dimension.height <= 0

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_width' AS column,
--       IF(product_dimension.width IS NULL, 'Column can not be NULL', IF(product_dimension.width = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base 
--   WHERE product_dimension.width IS NULL or product_dimension.width <= 0

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_category_id' AS column,
--       IF(category_ids.category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base 
--   WHERE category_ids.category_id IS NULL or category_ids.category_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_sub_category_id' AS column,
--       IF(category_ids.sub_category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE category_ids.sub_category_id IS NULL or category_ids.sub_category_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_sub_sub_category_id' AS column,
--       IF(category_ids.sub_sub_category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE category_ids.sub_sub_category_id IS NULL or category_ids.sub_sub_category_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'brand_id' AS column,
--       IF(brand_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE brand_id IS NULL or brand_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'external_id' AS column,
--       IF(external_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE external_id IS NULL or external_id = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_spesification' AS column,
--       IF(product_spesification IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_spesification IS NULL or product_spesification = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_description' AS column,
--       IF(product_description IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_description IS NULL or product_description = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_unit' AS column,
--       IF(product_unit IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_unit IS NULL or product_unit = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_weight' AS column,
--       IF(product_weight IS NULL, 'Column can not be NULL', IF(product_weight = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_weight IS NULL or product_weight <= 0 

--    UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_price' AS column,
--       IF(product_price IS NULL, 'Column can not be NULL', IF(product_price = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_price IS NULL or product_price <= 0 

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_minimum_order' AS column,
--       IF(product_minimum_order IS NULL, 'Column can not be NULL', IF(product_minimum_order = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_minimum_order IS NULL or product_minimum_order <= 0 

-- -- product_variant  (struct)

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_stock' AS column,
--       "Contains zero product_stock when value of on_shelf is TRUE" AS notes
--     ) quality_check
--   FROM base
--   WHERE
--     product_stock = 0 AND on_shelf = TRUE

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'on_shelf' AS column,
--       "on_shelf is TRUE when value of product_stock equals zero" AS notes
--     ) AS quality_check
--   FROM base
--   WHERE
--     product_stock = 0 AND on_shelf = TRUE

--   UNION ALL

--     SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'product_stock' AS column,
--       "Contains a number of product_stock when value of on_shelf is FALSE" AS notes
--     ) quality_check
--   FROM base
--   WHERE
--     product_stock > 0 AND on_shelf = FALSE

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'on_shelf' AS column,
--       "on_shelf is FALSE when value of product_stock equals non zero value" AS notes
--     ) AS quality_check
--   FROM base
--   WHERE
--     product_stock > 0 AND on_shelf = FALSE

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'is_tax' AS column,
--       IF(is_tax IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base 
--   WHERE is_tax IS NULL 

--     UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'is_bonus' AS column,
--       IF(is_bonus IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE is_bonus IS NULL 

-- )

-- ,aggregated_check AS (
--   SELECT 
--     product_id,
--     published_timestamp,
--     ARRAY_AGG(
--       quality_check
--     ) AS quality_check
--   FROM check
--   GROUP BY 1, 2
-- )

SELECT
  A.*
  -- B.quality_check
FROM
  base A
  -- LEFT JOIN aggregated_check B
  -- ON A.product_id = B.product_id
  -- AND A.published_timestamp = B.published_timestamp
