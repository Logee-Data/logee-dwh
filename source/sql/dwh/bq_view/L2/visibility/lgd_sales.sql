-- CHECK 
WITH base AS (
  SELECT *
  REPLACE(
    TO_HEX(SHA256(username)) AS username,
    TO_HEX(SHA256(email)) AS email,
    TO_HEX(SHA256(phone_number)) AS phone_number
    ) 
  FROM `logee-data-prod.L1_visibility.lgd_sales`
)

, check AS (

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
      'sales_code' AS column,
      IF(sales_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base 
  WHERE sales_code IS NULL or sales_code = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'sales_type' AS column,
      IF(sales_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sales_type IS NULL or sales_type = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'user_id' AS column,
      IF(user_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base 
  WHERE user_id IS NULL or user_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'user_type' AS column,
      IF(user_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_type IS NULL or user_type = ''

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
      'apps' AS column,
      IF(apps IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base 
  WHERE apps IS NULL or apps = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'employee_status' AS column,
      IF(employee_status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE employee_status IS NULL or employee_status = ''

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