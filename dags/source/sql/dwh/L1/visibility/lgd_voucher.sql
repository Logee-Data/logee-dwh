WITH base AS (
  SELECT * 
  FROM 
    `logee-data-prod.logee_datalake_raw_production.visibility_lgd_voucher` 
  WHERE
    _date_partition IN ('{{ ds }}', '{{ next_ds }}')
    AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- Begin company_ids
,company_ids AS (
  SELECT
    voucher_id,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(company_ids, '"', '')
    ) AS company_ids
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(voucher_id, '$.companyIds')) AS company_ids
  GROUP BY 1,2
)

-- End

-- BEGIN company_group_ids
,company_group_ids AS (
  SELECT
    voucher_id,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(company_group_ids , '"', '')
    ) AS company_group_ids 
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(voucher_id, '$.companyGroupIds')) AS company_group_ids 
  GROUP BY 1,2
)

-- End

SELECT
  REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherId'), '"', '') AS voucher_id,
  REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherCode'), '"', '') AS voucher_code,
  CAST(REPLACE(JSON_EXTRACT(A.voucher_id, '$.amount'), '"', '') AS FLOAT64) AS amount,
  REPLACE(JSON_EXTRACT(A.voucher_id, '$.orderId'), '"', '') AS order_id,
  B.company_ids,
  C.company_group_ids,
  IF(REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherTitle'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherTitle'), '"', '')) AS voucher_title,
  IF(REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherDescription'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherDescription'), '"', '')) AS voucher_description,
  REPLACE(JSON_EXTRACT(A.voucher_id, '$.voucherStatus'), '"', '') AS voucher_status,
  CAST(JSON_EXTRACT(A.voucher_id, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(A.voucher_id, '$.startDate'), '"', '') AS TIMESTAMP) AS start_date,
  CAST(REPLACE(JSON_EXTRACT(A.voucher_id, '$.endDate'), '"', '') AS TIMESTAMP) AS end_date,
  CAST(REPLACE(JSON_EXTRACT(A.voucher_id, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.voucher_id, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.voucher_id, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.voucher_id, '$.modifiedBy'), '"', '') AS modified_by,
  ts AS published_timestamp
FROM base A
 LEFT JOIN company_ids B
  ON A.voucher_id = B.voucher_id
  AND A.ts = B.published_timestamp

  LEFT JOIN company_group_ids C
  ON A.voucher_id = C.voucher_id
  AND A.ts = C.published_timestamp
