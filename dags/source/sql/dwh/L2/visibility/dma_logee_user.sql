-- CHECK

WITH base as(
    SELECT 
        * 
        REPLACE(
        TO_HEX(SHA256(phone)) AS phone,
        TO_HEX(SHA256(email)) AS email,
        STRUCT (
            TO_HEX(SHA256(user_meta.phone)) AS phone,
            TO_HEX(SHA256(user_meta.ktp_num)) AS ktp_num,
            TO_HEX(SHA256(user_meta.sim_num)) AS sim_num,
            user_meta.sim_type,
            user_meta.device_id,
            user_meta.driver_id,
            user_meta.company_id,
            user_meta.driver_status,
            user_meta.is_registering_from_app,
            user_meta.group_id,
            user_meta.session_id,
            TO_HEX(SHA256(user_meta.web_api_key)) AS web_api_key,
            TO_HEX(SHA256(user_meta.cust_id_ppjk)) AS cust_id_ppjk,
            TO_HEX(SHA256(user_meta.web_device_id)) AS web_device_id,
            TO_HEX(SHA256(user_meta.koja_password)) AS koja_password,
            TO_HEX(SHA256(user_meta.koja_username)) AS koja_username
            ) AS user_meta
    )
    FROM 
        `logee-data-prod.L1_visibility.dma_logee_user`
    WHERE
        modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- BEGIN CHECK
,check AS (
  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'user_id' AS column,
      IF(user_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_id IS NULL or user_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'phone' AS column,
      IF(phone IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE phone IS NULL or phone = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'email' AS column,
      IF(email IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE email IS NULL or email = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'name' AS column,
      IF(name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE name IS NULL or name = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'feature' AS column,
      IF(feature IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE feature IS NULL or feature = ''

  UNION ALL

   SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'avatar' AS column,
      IF(avatar IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE avatar IS NULL or avatar = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'unseen' AS column,
      IF(unseen IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE unseen IS NULL or unseen = ''

  UNION ALL

  -- START USER_META

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'phone' AS column,
      IF(user_meta.phone IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.phone IS NULL or user_meta.phone = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'ktp_num' AS column,
      IF(user_meta.ktp_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.ktp_num IS NULL or user_meta.ktp_num = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sim_num' AS column,
      IF(user_meta.sim_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.sim_num IS NULL or user_meta.sim_num = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sim_type' AS column,
      IF(user_meta.sim_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.sim_type IS NULL or user_meta.sim_type = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'device_id' AS column,
      IF(user_meta.device_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.device_id IS NULL or user_meta.device_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'driver_id' AS column,
      IF(user_meta.driver_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.driver_id IS NULL or user_meta.driver_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'company_id' AS column,
      IF(user_meta.company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.company_id IS NULL or user_meta.company_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'driver_status' AS column,
      IF(user_meta.driver_status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.driver_status IS NULL or user_meta.driver_status = ''

  UNION ALL

 SELECT
    user_id,
    published_timestamp,
    STRUCT(
      'is_registering_from_app' AS column,
       'Column can not be NULL' AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.is_registering_from_app IS NULL

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'group_id' AS column,
      IF(user_meta.group_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.group_id IS NULL or user_meta.group_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'session_id' AS column,
      IF(user_meta.session_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.session_id IS NULL or user_meta.session_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'web_api_key' AS column,
      IF(user_meta.web_api_key IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.web_api_key IS NULL or user_meta.web_api_key = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'cust_id_ppjk' AS column,
      IF(user_meta.cust_id_ppjk IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.cust_id_ppjk IS NULL or user_meta.cust_id_ppjk = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'web_device_id' AS column,
      IF(user_meta.web_device_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.web_device_id IS NULL or user_meta.web_device_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'koja_password' AS column,
      IF(user_meta.koja_password IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.koja_password IS NULL or user_meta.koja_password = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'koja_username' AS column,
      IF(user_meta.koja_username IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_meta.koja_username IS NULL or user_meta.koja_username = ''

  -- END USER_META
)
-- END CHECK

,aggregated_check AS (
  SELECT 
    user_id,
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
  ON A.user_id = B.user_id
  AND A.published_timestamp = B.published_timestamp
