WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.L1_visibility.lgd_brand`
)

-- BEGIN CHECK
,check AS (
  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'brand_id' AS column,
      IF(brand_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    brand_id IS NULL or brand_id = ''
  
  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'brand_name' AS column,
      IF(brand_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    brand_name IS NULL or brand_name = ''
)
-- END CHECK

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
  base A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp