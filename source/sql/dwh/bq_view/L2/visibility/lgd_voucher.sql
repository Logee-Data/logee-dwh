-- CHECK


WITH check AS (
  SELECT
  original_data,
  published_timestamp,

  STRUCT(
    'voucher_code' AS column,
    IF(voucher_code IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE voucher_code IS NULL or voucher_code = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'amount' AS column,
      IF(amount IS NULL, 'Column can not be NULL', IF(amount = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE amount IS NULL or amount <= 0

  UNION ALL

  SELECT
  original_data,
  published_timestamp,

  STRUCT(
    'order_id' AS column,
    IF(order_id IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE order_id IS NULL or order_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

  STRUCT(
    'voucher_title' AS column,
    IF(voucher_title IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE voucher_title IS NULL or voucher_title= ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

  STRUCT(
    'voucher_description' AS column,
    IF(voucher_description IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE voucher_description IS NULL or voucher_description = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

  STRUCT(
    'voucher_status' AS column,
    IF(voucher_status IS NULL, 'Column can not be Null', 'Column can not be an empty string') AS quality_notes
  ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
  WHERE voucher_status IS NULL or voucher_status = ''
  
  UNION ALL
  
  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_deleted' AS column,
      IF(is_deleted IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-prod.L1_visibility.lgd_voucher` 
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
A. *,
B.quality_check
FROM `logee-data-prod.L1_visibility.lgd_voucher` A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp
