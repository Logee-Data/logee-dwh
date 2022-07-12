WITH base AS (
  SELECT 
    *
  FROM 
    `logee-data-prod.L1_visibility.lgd_service_fee_pip`
  WHERE
    published_timestamp BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
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
--       'product_seller_id' AS column,
--       IF(product_seller_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base 
--   WHERE product_seller_id IS NULL or product_seller_id = ''

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
--       'weight' AS column,
--       IF(weight IS NULL, 'Column can not be NULL', IF(weight = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE weight IS NULL or weight <= 0 

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'weight_metrics' AS column,
--       IF(weight_metrics IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE weight_metrics IS NULL or weight_metrics = ''

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'price' AS column,
--       IF(price IS NULL, 'Column can not be NULL', IF(price = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE price IS NULL or price <= 0 

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'konstanta' AS column,
--       IF(konstanta IS NULL, 'Column can not be NULL', IF(konstanta = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE konstanta IS NULL or konstanta <= 0 

--   UNION ALL

--   SELECT
--     product_id,
--     published_timestamp,
--     STRUCT(
--       'revenue_sharing' AS column,
--       IF(revenue_sharing IS NULL, 'Column can not be NULL', IF(revenue_sharing = 0, 'Column can not be equal to zero', 'Column can not be a negative number')) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE revenue_sharing IS NULL or revenue_sharing <= 0 
--   )

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
  A.*,
  -- B.quality_check
FROM
  base A
  -- LEFT JOIN aggregated_check B
  -- ON A.product_id = B.product_id
  -- AND A.published_timestamp = B.published_timestamp