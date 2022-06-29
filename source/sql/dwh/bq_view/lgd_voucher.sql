WITH base AS (
  SELECT * 
  FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_voucher` 
  WHERE 
  _date_partition IN ('{{ ds }}', '{{ next_ds }}')
  AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- Begin company_ids
,company_ids AS (
  SELECT
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(company_ids, '"', '')
    ) AS company_ids
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.companyIds')) AS company_ids
    GROUP BY 1
)

-- End

-- BEGIN company_group_ids
,company_group_ids AS (
  SELECT
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(company_group_ids , '"', '')
    ) AS company_group_ids 
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.companyGroupIds')) AS company_group_ids 
    GROUP BY 1
)

-- End

SELECT 
  REPLACE(JSON_EXTRACT(A.data, '$.voucherCode'), '"', '') AS voucher_code,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.amount'), '"', '') AS FLOAT64) AS amount,
  REPLACE(JSON_EXTRACT(A.data, '$.orderId'), '"', '') AS order_id,
  B.company_ids,
  C.company_group_ids,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.voucherTitle'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.voucherTitle'), '"', '')) AS voucher_title,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.voucherDescription'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.voucherDescription'), '"', '')) AS voucher_description,
  REPLACE(JSON_EXTRACT(A.data, '$.voucherStatus'), '"', '') AS voucher_status,
  CAST(JSON_EXTRACT(A.data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.startDate'), '"', '') AS TIMESTAMP) AS start_date,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.endDate'), '"', '') AS TIMESTAMP) AS end_date,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  ts AS published_timestamp
FROM base A
  LEFT JOIN company_ids B
  ON A.ts = B.published_timestamp

  LEFT JOIN company_group_ids C
  ON  A.ts = C.published_timestamp