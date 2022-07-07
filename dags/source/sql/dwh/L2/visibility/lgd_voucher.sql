-- CHECK

WITH base AS (
  SELECT 
    *
  FROM 
    `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)


,check AS (
  SELECT
  voucher_code,
  published_timestamp,

  STRUCT(
    'voucher_code' AS column,
    IF(voucher_code IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM base
  WHERE voucher_code IS NULL or voucher_code = ''

  UNION ALL

  SELECT
    voucher_code,
    published_timestamp,

    STRUCT(
      'amount' AS column,
      IF(amount IS NULL, 'Column can not be NULL', IF(amount = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM base
  WHERE amount IS NULL or amount <= 0

  UNION ALL

  SELECT
    voucher_code,
    published_timestamp,

  STRUCT(
    'order_id' AS column,
    IF(order_id IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM base
  WHERE order_id IS NULL or order_id = ''

  UNION ALL

  SELECT
    voucher_code,
    published_timestamp,

  STRUCT(
    'voucher_title' AS column,
    IF(voucher_title IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM base
  WHERE voucher_title IS NULL or voucher_title= ''

  UNION ALL

  SELECT
    voucher_code,
    published_timestamp,

  STRUCT(
    'voucher_description' AS column,
    IF(voucher_description IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM base
  WHERE voucher_description IS NULL or voucher_description = ''

  UNION ALL

  SELECT
    voucher_code,
    published_timestamp,

  STRUCT(
    'voucher_status' AS column,
    IF(voucher_status IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM base
  WHERE voucher_status IS NULL or voucher_status = ''
  
)


,aggregated_check AS (
  SELECT 
    voucher_code,
    published_timestamp,
    ARRAY_AGG(
      quality_check
    ) AS quality_check
  FROM check
  GROUP BY 1, 2
)

SELECT 
A. *,
B.quality_check
FROM base A
  LEFT JOIN aggregated_check B
  ON A.voucher_code = B.voucher_code
  AND A.published_timestamp = B.published_timestamp
