WITH 

base AS (
  SELECT * FROM `logee-data-prod.logee_datalake_raw_production.visibility_dma_logee_user` 
  WHERE _date_partition = "2022-06-20" LIMIT 1000
)

-- Begin apps
,apps AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(apps, '"', '')
    ) AS apps
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.apps')) AS apps
  GROUP BY 1,2
)
-- End


-- Begin roles
,roles AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(roles, '"', '')
    ) AS roles
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.apps')) AS roles
  GROUP BY 1,2
)
-- End

SELECT 
  IF(REPLACE(JSON_EXTRACT(A.data, '$.phone'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.phone'), '"', '')) AS phone,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '')) AS email,
  REPLACE(JSON_EXTRACT(A.data, '$.name'), '"', '') AS name,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.feature'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.feature'), '"', '')) AS feature,
  REPLACE(JSON_EXTRACT(A.data, '$.avatar'), '"', '') AS avatar,
  CAST(JSON_EXTRACT(A.data, '$.assignIndicar') AS BOOLEAN) AS assign_indicar,
  CAST(JSON_EXTRACT(A.data, '$.showTracking') AS BOOLEAN) AS show_tracking,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.unseen'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.unseen'), '"', '')) AS unseen,
  STRUCT(
    IF(REPLACE(JSON_EXTRACT(A.data, '$.userMeta.phone'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.userMeta.phone'), '"', '')) AS phone,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.ktpNum'), '"', '') AS ktp_num,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.simNum'), '"', '') AS sim_num,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.simType'), '"', '') AS sim_type,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.userMeta.deviceId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.userMeta.deviceId'), '"', '')) AS device_id,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.userMeta.driverId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.userMeta.driverId'), '"', '')) AS driver_id,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.userMeta.companyId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.userMeta.companyId'), '"', '')) AS company_id,
    IF(JSON_EXTRACT(A.data, '$.userMeta.driverStatus')  = "", NULL, JSON_EXTRACT(A.data, '$.driverStatus')) AS driver_status,
    CAST(JSON_EXTRACT(A.data, '$.userMeta.isRegisteringFromApp') AS BOOLEAN) AS is_registering_from_app,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.groupId'), '"', '')AS group_id,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.sessionId'), '"', '')AS session_id,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.webApiKey'), '"', '') AS web_api_key,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.custIdPpjk'), '"', '') AS cust_id_ppjk,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.webDeviceId'), '"', '') AS web_device_id,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.kojaPassword'), '"', '') AS koja_password,
    REPLACE(JSON_EXTRACT(A.data, '$.userMeta.kojaUsername'), '"', '')AS koja_username
  ) AS user_meta,
  B.apps,
  C.roles,
  CAST(JSON_EXTRACT(A.data, '$.isActive') AS BOOLEAN) AS is_active,
  CAST(JSON_EXTRACT(A.data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  REPLACE(JSON_EXTRACT(A.data, '$.userId'), '"', '') AS user_id,
  A.data AS original_data,
  ts AS published_timestamp

FROM base A
  LEFT JOIN apps B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN roles C
  ON A.data = C.data
  AND A.ts = C.published_timestamp
