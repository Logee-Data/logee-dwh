WITH base AS (
  SELECT *
  REPLACE(
    TO_HEX(SHA256(sales_name)) AS sales_name
    ) 
  FROM `logee-data-prod.L1_visibility.lgd_attendance`
)

, check AS (

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'attendance_id' AS column,
      IF(attendance_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE attendance_id IS NULL or attendance_id = ''

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'company_id' AS column,
      IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE company_id IS NULL or company_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'sales_id' AS column,
      IF(sales_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sales_id IS NULL or sales_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'sales_name' AS column,
      IF(sales_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sales_name IS NULL or sales_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'status' AS column,
      IF(status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE status IS NULL or status = ''  

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_deleted' AS column,
      IF(is_deleted IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
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
  base A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp