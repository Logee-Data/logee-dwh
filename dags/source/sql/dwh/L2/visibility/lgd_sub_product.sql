WITH base AS (
  SELECT 
    *
  FROM 
    `logee-data-prod.L1_visibility.lgd_sub_product`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)


-- , check AS (

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_id' AS column,
--       IF(sub_product_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_id IS NULL or sub_product_id = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'company_id' AS column,
--       IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_id IS NULL or company_id = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'brand_id' AS column,
--       IF(brand_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE brand_id IS NULL or brand_id = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'category_id' AS column,
--       IF(category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE category_id IS NULL or category_id = ''

--   UNION ALL

--    SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'product_id' AS column,
--       IF(product_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE product_id IS NULL or product_id = ''

--   UNION ALL

--  SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_name' AS column,
--       IF(sub_product_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_name IS NULL or sub_product_name = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_size' AS column,
--       IF(sub_product_size IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_size IS NULL or sub_product_size = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_discount_percent' AS column,
--       IF(sub_product_discount_percent IS NULL, 'Column can not be NULL', IF(sub_product_discount_percent = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_discount_percent IS NULL or sub_product_discount_percent <= 0

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_description' AS column,
--       IF(sub_product_description IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_description IS NULL or sub_product_description = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_unit' AS column,
--       IF(sub_product_unit IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_unit IS NULL or sub_product_unit = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_price' AS column,
--       IF(sub_product_price IS NULL, 'Column can not be NULL', IF(sub_product_price = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_price IS NULL or sub_product_price <= 0

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_weight' AS column,
--       IF(sub_product_weight IS NULL, 'Column can not be NULL', IF(sub_product_weight = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_weight IS NULL or sub_product_weight <= 0

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'external_id' AS column,
--       IF(external_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE external_id IS NULL or external_id = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_stock' AS column,
--       "Contains zero sub_product_stock when value of on_shelf is TRUE" AS notes
--     ) quality_check
--   FROM base
--   WHERE
--     sub_product_stock = 0 AND on_shelf = TRUE

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_stock' AS column,
--       "Contains a number of sub_product_stock when value of on_shelf is FALSE" AS notes
--     ) quality_check
--   FROM base
--   WHERE
--     sub_product_stock > 0 AND on_shelf = FALSE

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_stock' AS column,
--       "Contains zero sub_product_stock when value of product_on_shelf is TRUE" AS notes
--     ) quality_check
--   FROM base
--   WHERE
--     sub_product_stock = 0 AND product_on_shelf = TRUE

--   UNION ALL

-- SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_stock' AS column,
--       "Contains a number of sub_product_stock when value of product_on_shelf is FALSE" AS notes
--     ) quality_check
--   FROM base
--   WHERE
--     sub_product_stock > 0 AND product_on_shelf = FALSE

--   UNION ALL
  
--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'on_shelf' AS column,
--       "on_shelf is TRUE when value of sub_product_stock equals zero" AS notes
--     ) AS quality_check
--   FROM base
--   WHERE
--     sub_product_stock = 0 AND on_shelf = TRUE

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'on_shelf' AS column,
--       "on_shelf is FALSE when value of sub_product_stock equals non zero value" AS notes
--     ) AS quality_check
--   FROM base
--   WHERE
--     sub_product_stock > 0 AND on_shelf = FALSE

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'product_on_shelf' AS column,
--       "product_on_shelf is TRUE when value of sub_product_stock equals zero" AS notes
--     ) AS quality_check
--   FROM base
--   WHERE
--     sub_product_stock = 0 AND product_on_shelf = TRUE

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'product_on_shelf' AS column,
--       "product_on_shelf is FALSE when value of sub_product_stock equals non zero value" AS notes
--     ) AS quality_check
--   FROM base
--   WHERE
--     sub_product_stock > 0 AND product_on_shelf = FALSE

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_products_size' AS column,
--       IF(sub_products_size IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_products_size IS NULL or sub_products_size = ''

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_minimum_order' AS column,
--       IF(sub_product_minimum_order IS NULL, 'Column can not be NULL', IF(sub_product_minimum_order = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_minimum_order IS NULL or sub_product_minimum_order <= 0 

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'sub_product_stock_on_hold' AS column,
--       IF(sub_product_stock_on_hold IS NULL, 'Column can not be NULL', IF(sub_product_stock_on_hold = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_product_stock_on_hold IS NULL or sub_product_stock_on_hold <= 0 

--   UNION ALL

--   SELECT
--     sub_product_id,
--     published_timestamp,
--     STRUCT(
--       'is_tax' AS column,
--       IF(is_tax IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE is_tax IS NULL 

--     UNION ALL

--   SELECT
--     sub_product_id,
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
--     sub_product_id,
--     published_timestamp,
--     ARRAY_AGG(
--       quality_check
--     ) AS quality_check
--   FROM check
--   GROUP BY 1, 2
-- )

SELECT
  A.*
 --  , B.quality_check
FROM
  base A
 --  LEFT JOIN aggregated_check B
 --  ON A.sub_product_id = B.sub_product_id
 --  AND A.published_timestamp = B.published_timestamp