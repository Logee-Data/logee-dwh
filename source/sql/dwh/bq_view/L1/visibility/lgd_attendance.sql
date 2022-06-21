WITH BASE AS (
  SELECT *
  FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_attendance`
  WHERE _date_partition >= "2022-01-01" 
)

,pre_track_activities AS (
  SELECT
  data AS original_data,
  ts AS published_timestamp,
  ARRAY_AGG(
    STRUCT(
      JSON_EXTRACT_SCALAR(pre_track_activities, '$.type') AS track_activities_type,
      CAST(JSON_EXTRACT_SCALAR(pre_track_activities, '$.timestamp') AS TIMESTAMP) AS track_activities_timestamp,
      STRUCT(
        IF(REPLACE(JSON_EXTRACT(pre_track_activities, '$.location.lat'), '"', '') = '', NULL, CAST(REPLACE(JSON_EXTRACT(pre_track_activities, '$.location.lat'), '"', '') AS FLOAT64)) AS lat,
        IF(REPLACE(JSON_EXTRACT(pre_track_activities, '$.location.long'), '"', '') = '', NULL, CAST(REPLACE(JSON_EXTRACT(pre_track_activities, '$.location.long'), '"', '') AS FLOAT64)) AS long
      ) AS location
    )
  ) AS track_activities
  FROM BASE,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data,'$.trackActivities'), '$.')) AS pre_track_activities
  GROUP BY 1,2
)

, pre_attendances AS (
  SELECT
  data AS original_data,
  ts AS published_timestamp,
  ARRAY_AGG(
    STRUCT(
      REPLACE(JSON_EXTRACT(pre_attendances, '$.status'),'"','') AS attendances_status,
      DATE(JSON_EXTRACT_SCALAR(pre_attendances, '$.date')) AS attendances_date,
      STRUCT(
        REPLACE(JSON_EXTRACT(pre_attendances, '$.location.address'),'"','') AS attendances_location_address,
        IF(REPLACE(JSON_EXTRACT(pre_attendances, '$.location.lat'), '"', '') = '', NULL, CAST(REPLACE(JSON_EXTRACT(pre_attendances, '$.location.lat'), '"', '') AS FLOAT64)) AS lat,
        IF(REPLACE(JSON_EXTRACT(pre_attendances, '$.location.long'), '"', '') = '', NULL, CAST(REPLACE(JSON_EXTRACT(pre_attendances, '$.location.long'), '"', '') AS FLOAT64)) AS long
      ) AS location
    ) 
  ) AS attendances  
  FROM BASE,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data,'$.attendances'), '$.')) AS pre_attendances
  GROUP by 1,2
)

SELECT 
  REPLACE(JSON_EXTRACT(data, '$.attendanceId'),'"','') AS attendance_id,
  REPLACE(JSON_EXTRACT(data, '$.companyId'),'"','') AS company_id,
  REPLACE(JSON_EXTRACT(data, '$.salesId'),'"','') AS sales_id,
  REPLACE(JSON_EXTRACT(data, '$.salesName'),'"','') AS sales_name,
  REPLACE(JSON_EXTRACT(data, '$.status'),'"','') AS status,
  B.track_activities,
  C.attendances,
  DATE(JSON_EXTRACT_SCALAR(data, '$.date')) AS date,
  CAST(JSON_EXTRACT(data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  REPLACE(JSON_EXTRACT(data, '$.createdBy'),'"','') AS created_by,
  CAST(JSON_EXTRACT_SCALAR(data, '$.createdAt') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(data, '$.modifiedBy'),'"','') AS modified_by,
  CAST(JSON_EXTRACT_SCALAR(data, '$.modifiedAt') AS TIMESTAMP) AS modified_at,
  data AS original_data,
  ts AS published_timestamp
FROM
  BASE A
FULL OUTER JOIN pre_track_activities B
ON A.data = B.original_data
AND A.ts = B.published_timestamp

FULL OUTER JOIN pre_attendances C
ON A.data = C.original_data
AND A.ts = C.published_timestamp