WITH base AS (
  SELECT
  *
  REPLACE(
    TO_HEX(SHA256(name)) AS name,
    TO_HEX(SHA256(phone)) AS phone,
    TO_HEX(SHA256(email)) AS email
    -- TO_HEX(SHA256(date_of_birth)) AS date_of_birth,
    -- TO_HEX(SHA256(sim_expired)) AS sim_expired

  )
  FROM
    `logee-data-dev.L1_visibility.dma_logee_drivers`
)

,check AS (
  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'driver_id' AS column,
      IF(driver_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE driver_id IS NULL or driver_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'place_of_birth' AS column,
      IF(place_of_birth IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE place_of_birth IS NULL or place_of_birth = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'ktp_num' AS column,
      IF(ktp_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE ktp_num IS NULL or ktp_num = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'sim_type' AS column,
      IF(sim_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE sim_type IS NULL or sim_type = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'sim_num' AS column,
      IF(sim_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE sim_num IS NULL or sim_num = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'address' AS column,
      IF(address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE address IS NULL or address = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'company_id' AS column,
      IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE company_id IS NULL or company_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'company_name' AS column,
      IF(company_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_drivers`
  WHERE company_name IS NULL or company_name = ''


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
  `logee-data-dev.L1_visibility.dma_logee_drivers`  A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp