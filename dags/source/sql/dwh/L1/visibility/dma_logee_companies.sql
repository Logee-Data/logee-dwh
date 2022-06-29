WITH

base AS (
  SELECT * 
  FROM 
    `logee-data-prod.logee_datalake_raw_production.visibility_dma_logee_companies` 
  WHERE
    _date_partition IN ('{{ ds }}', '{{ next_ds }}')
    AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- BEGIN allowedPaymentType
,allowed_payment_type AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      DISTINCT REPLACE(allowed_payment_type, '"', '')
    ) AS allowed_payment_type
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.partnerIds')) AS allowed_payment_type
  GROUP BY 1,2
)
-- END


-- BEGIN partner_ids
,partner_ids AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      DISTINCT REPLACE(partner_ids, '"', '')
    ) AS partner_ids
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.partnerIds')) AS partner_ids
  GROUP BY 1,2
)
-- END

-- BEGIN links
,pre_links AS (
  SELECT
  data,
  ts AS published_timestamp,
  STRUCT(
     REPLACE(JSON_EXTRACT(links, '$.link'), '"', '') AS link,
     REPLACE(JSON_EXTRACT(links, '$.type'), '"', '') AS type
  ) AS links
  FROM base,
   UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.visibility'), '$.links')) AS links
)

,links AS (
  SELECT
  data,
  published_timestamp,
  ARRAY_AGG(links) AS links
  from pre_links
  GROUP BY 1, 2
)
-- END


-- BEGIN visibility
,visibility AS (
  SELECT
  A.data,
  ts AS published_timestamp,
    STRUCT (
      D.links,
      REPLACE(JSON_EXTRACT(A.data, '$.visibility.feature'), '"', '') AS feature,
      REPLACE(JSON_EXTRACT(A.data, '$.visibility.document'), '"', '') AS document,
      CAST(REPLACE(JSON_EXTRACT(A.data, '$.visibility.updatedAt'), '"', '') AS TIMESTAMP) AS updated_at
    ) AS visibility
  FROM base A
    LEFT JOIN links D
    ON A.data = D.data
    AND A.ts = D.published_timestamp

)
-- END


-- Begin Control
,control AS (
  SELECT
  A.data,
  ts AS published_timestamp,
    STRUCT (
      D.links,
      REPLACE(JSON_EXTRACT(A.data, '$.control.feature'), '"', '') AS feature,
      REPLACE(JSON_EXTRACT(A.data, '$.control.document'), '"', '') AS document,
      CAST(REPLACE(JSON_EXTRACT(A.data, '$.control.updatedAt'), '"', '') AS TIMESTAMP) AS updated_at
    ) AS control
  FROM base A
    LEFT JOIN links D
    ON A.data = D.data
    AND A.ts = D.published_timestamp

)
-- End


SELECT 
  REPLACE(JSON_EXTRACT(A.data, '$.companyId'), '"', '') AS company_id,
  REPLACE(JSON_EXTRACT(A.data, '$.name'), '"', '') AS name,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.type'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.type'), '"', '')) AS type,
  REPLACE(JSON_EXTRACT(A.data, '$.address'), '"', '') AS address,
  REPLACE(JSON_EXTRACT(A.data, '$.cityId'), '"', '') AS city_id,
  REPLACE(JSON_EXTRACT(A.data, '$.cityName'), '"', '') AS city_name,
  REPLACE(JSON_EXTRACT(A.data, '$.provinceId'), '"', '') AS province_id,
  REPLACE(JSON_EXTRACT(A.data, '$.provinceName'), '"', '') AS province_name,
  REPLACE(JSON_EXTRACT(A.data, '$.districtId'), '"', '') AS district_id,
  REPLACE(JSON_EXTRACT(A.data, '$.districtName'), '"', '') AS district_name,
  REPLACE(JSON_EXTRACT(A.data, '$.subdistrictId'), '"', '')AS sub_district_id,
  REPLACE(JSON_EXTRACT(A.data, '$.subdistrictName'), '"', '') AS sub_district_name,
  REPLACE(JSON_EXTRACT(A.data, '$.zipCode'), '"', '') AS zip_code,
  REPLACE(JSON_EXTRACT(A.data, '$.phone'), '"', '') AS phone,
  REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '') AS email,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.website'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.website'), '"', '')) AS website,
  REPLACE(JSON_EXTRACT(A.data, '$.npwpNum'), '"', '') AS npwp_num,
  REPLACE(JSON_EXTRACT(A.data, '$.siupNum'), '"', '') AS siup_num,
  DATE(REPLACE(JSON_EXTRACT(A.data, '$.siupExpiryDate'), '"', '')) AS siup_expiry_date,
  REPLACE(JSON_EXTRACT(A.data, '$.directorKtpNum'), '"', '') AS director_ktp_num,
  REPLACE(JSON_EXTRACT(A.data, '$.directorName'), '"', '') AS director_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.creditLimit'), '"', '') AS FLOAT64) AS credit_Limit,
  REPLACE(JSON_EXTRACT(A.data, '$.partnershipType'), '"', '') AS partnership_type,
  STRUCT(
    REPLACE(JSON_EXTRACT(A.data, '$.companyDocument.npwpFile'), '"', '') AS npwp_file,
    REPLACE(JSON_EXTRACT(A.data, '$.companyDocument.siupFile'), '"', '') AS siup_file,
    REPLACE(JSON_EXTRACT(A.data, '$.companyDocument.directorKtpFile'), '"', '') AS director_ktp_file
  ) AS company_document,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.latitude'), '"', '') AS FLOAT64) AS latitude,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.longitude'), '"', '') AS FLOAT64) AS longitude,
  B.partner_ids,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.pksNum'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.pksNum'), '"', '')) AS pks_num,
  DATE(REPLACE(JSON_EXTRACT(A.data, '$.pksExpiryDate'), '"', '')) AS pks_expiry_date,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.availableCredit'), '"', '') AS FLOAT64) AS available_credit,
  REPLACE(JSON_EXTRACT(A.data, '$.mandiriInvoiceId'), '"', '') AS mandiri_invoice_id,
   C.allowed_payment_type,
  REPLACE(JSON_EXTRACT(A.data, '$.avatar'), '"', '') AS avatar,
  REPLACE(JSON_EXTRACT(A.data, '$.form'), '"', '') AS form,
  REPLACE(JSON_EXTRACT(A.data, '$.dueDate'), '"', '') AS due_date,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.paymentType'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.paymentType'), '"', '')) AS payment_type,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.bankName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.bankName'), '"', '')) AS bank_name,
  REPLACE(JSON_EXTRACT(A.data, '$.bankTransferCode'), '"', '') AS bank_transfer_code,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.feePercentage'), '"', '') AS FLOAT64) AS fee_percentage,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.salesId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.salesId'), '"', '')) AS sales_id,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.salesName'), '"', '') = "-", NULL, REPLACE(JSON_EXTRACT(A.data, '$.salesName'), '"', '')) AS sales_name,
  CAST(JSON_EXTRACT(A.data, '$.isActive') AS BOOLEAN) AS is_active,
  CAST(JSON_EXTRACT(A.data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.bankAccountName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.bankAccountName'), '"', '')) AS bank_account_name,
  REPLACE(JSON_EXTRACT(A.data, '$.bankAccountNum'), '"', '') AS bank_account_num,
  REPLACE(JSON_EXTRACT(A.data, '$.tracking'), '"', '') AS tracking,
  E.visibility,
  F.control,
  ts AS published_timestamp
FROM base A
  LEFT JOIN partner_ids B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN allowed_payment_type C
  ON A.data = C.data
  AND A.ts = C.published_timestamp

  LEFT JOIN visibility E
  ON A.data = E.data
  AND A.ts = E.published_timestamp

  LEFT JOIN control F
  ON A.data = F.data
  AND A.ts = F.published_timestamp 
