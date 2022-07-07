WITH base AS (
  SELECT
    *
    REPLACE(
      TO_HEX(SHA256(name)) AS name,
      TO_HEX(SHA256(phone)) AS phone,
      TO_HEX(SHA256(email)) AS email,
      TO_HEX(SHA256(ktp_num)) AS ktp_num,
      TO_HEX(SHA256(sim_num)) AS sim_num,
      TO_HEX(SHA256(address)) AS address,
      TO_HEX(SHA256(image_sim)) AS image_sim,
      TO_HEX(SHA256(image_ktp)) AS image_ktp,
      TO_HEX(SHA256(avatar)) AS avatar,
      TO_HEX(SHA256(STRING(date_of_birth))) AS date_of_birth
    )
  FROM
    `logee-data-prod.L1_visibility.dma_logee_drivers`
  -- WHERE
  --   modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

,check AS (
  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'driver_id' AS column,
      IF(driver_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE driver_id IS NULL or driver_id = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'name' AS column,
      IF(name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE name IS NULL or name = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'phone' AS column,
      IF(phone IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE phone IS NULL or phone = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'email' AS column,
      IF(email IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE email IS NULL or email = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'image_sim' AS column,
      IF(image_sim IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE image_sim IS NULL or image_sim = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'image_ktp' AS column,
      IF(image_ktp IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE image_ktp IS NULL or image_ktp = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'avatar' AS column,
      IF(avatar IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE avatar IS NULL or avatar = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'driver_status' AS column,
      'Column can not be NULL' AS quality_notes
    ) AS quality_check
  FROM base
  WHERE driver_status IS NULL

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'modified_by' AS column,
      IF(modified_by IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE modified_by IS NULL or modified_by = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'date_of_birth' AS column,
      'Column can not be NULL' AS quality_notes
    ) AS quality_check
  FROM base
  WHERE date_of_birth IS NULL

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'sim_expired' AS column,
      'Column can not be NULL' AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sim_expired IS NULL

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'place_of_birth' AS column,
      IF(place_of_birth IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE place_of_birth IS NULL or place_of_birth = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'ktp_num' AS column,
      IF(ktp_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE ktp_num IS NULL or ktp_num = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'sim_type' AS column,
      IF(sim_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sim_type IS NULL or sim_type = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'sim_num' AS column,
      IF(sim_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sim_num IS NULL or sim_num = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'address' AS column,
      IF(address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE address IS NULL or address = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'company_id' AS column,
      IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE company_id IS NULL or company_id = ''

  UNION ALL

  SELECT
    driver_id,
    published_timestamp,
    STRUCT(
      'company_name' AS column,
      IF(company_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE company_name IS NULL or company_name = ''


  )

,aggregated_check AS (
  SELECT
    driver_id,
    CAST(published_timestamp AS TIMESTAMP) AS published_timestamp,
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
  ON A.driver_id = B.driver_id
  AND A.published_timestamp = B.published_timestamp