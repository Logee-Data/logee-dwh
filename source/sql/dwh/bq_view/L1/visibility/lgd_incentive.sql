WITH base AS (
  SELECT *
  FROM `logee-data-dev.logee_datalake_raw_development.visibility_lgd_incentive`
  WHERE _date_partition >= "2022-01-01"
)

,pre_tier AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT(
        REPLACE(JSON_EXTRACT(pre_tier, '$.minimumTargetPercent'), '"', '') AS minimum_target_percent,
        REPLACE(JSON_EXTRACT(pre_tier, '$.incentivePercent'), '"', '') AS incentive_percent
      )
    ) AS tier
  FROM base,
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.tier')) AS pre_tier
  GROUP BY 1,2
)

SELECT
  REPLACE(JSON_EXTRACT(A.data, '$.incentiveId'), '"', '') AS incentive_id,
  REPLACE(JSON_EXTRACT(A.data, '$.companyId'), '"', '') AS company_id,
  REPLACE(JSON_EXTRACT(A.data, '$.salesId'), '"', '') AS sales_id,
  B.tier,
  CAST(JSON_EXTRACT(A.data, '$.isDefault') AS BOOLEAN) AS is_default,
  CAST(JSON_EXTRACT(A.data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '')) AS created_by,
  CAST(IF(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '')) AS TIMESTAMP) AS created_at,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '')) AS modified_by,
  CAST(IF(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '')) AS TIMESTAMP) AS modified_at,
  A.data,
  B.published_timestamp
FROM
  base A
  LEFT JOIN pre_tier B
  ON A.data = B.data
  AND A.ts = B.published_timestamp