WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.logee_datalake_raw_production.visibility_dma_logee_drivers`
  WHERE
   _date_partition IN ('{{ ds }}', '{{ next_ds }}')
    AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

SELECT
  IF(REPLACE(JSON_EXTRACT(data, '$.driverId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.driverId'), '"', ''))  AS driver_id,
  IF(REPLACE(JSON_EXTRACT(data, '$.name'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.name'), '"', ''))  AS name,
  IF(REPLACE(JSON_EXTRACT(data, '$.phone'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.phone'), '"', ''))  AS phone,
  IF(REPLACE(JSON_EXTRACT(data, '$.email'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.email'), '"', ''))  AS email,
  IF(REPLACE(JSON_EXTRACT(data, '$.placeOfBirth'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.placeOfBirth'), '"', ''))  AS place_of_birth,
  DATE(IF(REPLACE(JSON_EXTRACT(data, '$.dateOfBirth'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.dateOfBirth'), '"', ''))) AS date_of_birth,
  IF(REPLACE(JSON_EXTRACT(data, '$.ktpNum'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.ktpNum'), '"', '')) AS ktp_num,
  IF(REPLACE(JSON_EXTRACT(data, '$.simType'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.simType'), '"', '')) AS sim_type,
  IF(REPLACE(JSON_EXTRACT(data, '$.simNum'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.simNum'), '"', '')) AS sim_num,
  IF(REPLACE(JSON_EXTRACT(data, '$.address'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.address'), '"', '')) AS address,
  IF(REPLACE(JSON_EXTRACT(data, '$.companyId'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.companyId'), '"', '')) AS company_id,
  IF(REPLACE(JSON_EXTRACT(data, '$.companyName'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.companyName'), '"', '')) AS company_name,
  IF(IF(REPLACE(JSON_EXTRACT(data, '$.simExpiryDate'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.simExpiryDate'), '"', '')) NOT LIKE '+%',
      DATE(IF(REPLACE(JSON_EXTRACT(data, '$.simExpiryDate'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.simExpiryDate'), '"', ''))),
      NULL
  ) AS sim_expiry_date,
  IF(REPLACE(JSON_EXTRACT(data, '$.imageSim'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.imageSim'), '"', '')) AS image_sim,
  IF(REPLACE(JSON_EXTRACT(data, '$.imageKtp'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.imageKtp'), '"', '')) AS image_ktp,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.driverStatus'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.driverStatus'), '"', '')) AS BOOL) AS driver_status,
  IF(REPLACE(JSON_EXTRACT(data, '$.avatar'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.avatar'), '"', '')) AS avatar,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.isActive'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.isActive'), '"', '')) AS BOOL) AS is_active,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.isDeleted'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.isDeleted'), '"', '')) AS BOOL) AS is_deleted,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.createdAt'), '"', '')) AS TIMESTAMP) AS created_at,
  IF(REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.createdBy'), '"', '')) AS created_by,
  CAST(IF(REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.modifiedAt'), '"', '')) AS TIMESTAMP) AS modified_at,
  IF(REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(data, '$.modifiedBy'), '"', '')) AS modified_by,
  CAST(ts AS TIMESTAMP) AS published_timestamp
FROM
  base