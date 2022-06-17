WITH

base AS (
  SELECT * FROM `logee-data-prod.logee_datalake_raw_production.visibility_dma_logee_companies` 
  WHERE _date_partition >= "2022-01-01" 
)

-- BEGIN allowedPaymentType
,allowed_payment_type AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(allowed_payment_type, '"', '')
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
      REPLACE(partner_ids, '"', '')
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
  REPLACE(JSON_EXTRACT(A.data, '$.type'), '"', '') AS type,
  REPLACE(JSON_EXTRACT(A.data, '$.address'), '"', '') AS address,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.cityId'), '"', '') AS INT64) AS cityId,
  REPLACE(JSON_EXTRACT(A.data, '$.cityName'), '"', '') AS city_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.province_id'), '"', '') AS INT64) AS province_id,
  REPLACE(JSON_EXTRACT(A.data, '$.provinceName'), '"', '') AS province_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.district_id'), '"', '') AS INT64) AS district_id,
  REPLACE(JSON_EXTRACT(A.data, '$.districtName'), '"', '') AS district_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.subdistrictId'), '"', '') AS INT64) AS sub_district_id,
  REPLACE(JSON_EXTRACT(A.data, '$.subdistrictName'), '"', '') AS sub_district_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.zipCode'), '"', '') AS INT64) AS zip_code,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.phone'), '"', '') AS INT64) AS phone,
  REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '') AS email,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.website'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.website'), '"', '')) AS website,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.npwpNum'), '"', '') AS INT64) AS npwp_num,
  REPLACE(JSON_EXTRACT(A.data, '$.siupNum'), '"', '') AS siup_num,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.siupExpiryDate'), '"', '') AS TIMESTAMP) AS siup_expiry_date,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.directorKtpNum'), '"', '') AS INT64) AS director_ktp_num,
  REPLACE(JSON_EXTRACT(A.data, '$.directorName'), '"', '') AS director_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.creditLimit'), '"', '') AS FLOAT64) AS credit_Limit,
  REPLACE(JSON_EXTRACT(A.data, '$.partnershipType'), '"', '') AS partnership_type,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.latitude'), '"', '') AS INT64) AS latitude,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.longitude'), '"', '') AS INT64) AS longitude,
  REPLACE(JSON_EXTRACT(A.data, '$.pksNum'), '"', '') AS pks_num,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.pksExpiryDate'), '"', '') AS TIMESTAMP) AS pks_expiry_date,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.availableCredit'), '"', '') AS FLOAT64) AS available_credit,
  REPLACE(JSON_EXTRACT(A.data, '$.mandiriInvoiceId'), '"', '') AS mandiri_invoice_id,
  REPLACE(JSON_EXTRACT(A.data, '$.avatar'), '"', '') AS avatar,
  REPLACE(JSON_EXTRACT(A.data, '$.form'), '"', '') AS form,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.dueDate'), '"', '') AS INT64) AS due_date,
  REPLACE(JSON_EXTRACT(A.data, '$.paymentType'), '"', '') AS payment_type,
  REPLACE(JSON_EXTRACT(A.data, '$.bankName'), '"', '') AS bank_name,
  REPLACE(JSON_EXTRACT(A.data, '$.bankAccountName'), '"', '') AS bank_account_name,
  REPLACE(JSON_EXTRACT(A.data, '$.bankAccountNum'), '"', '') AS bank_account_num,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.bankTransferCode'), '"', '') AS INT64) AS bank_transfer_code,
  REPLACE(JSON_EXTRACT(A.data, '$.feePercentage'), '"', '') AS fee_percentage,
  STRUCT(
    REPLACE(JSON_EXTRACT(A.data, '$.companyDocument.npwpFile'), '"', '') AS npwp_file,
    REPLACE(JSON_EXTRACT(A.data, '$.companyDocument.siupFile'), '"', '') AS siup_file,
    REPLACE(JSON_EXTRACT(A.data, '$.companyDocument.directorKtpFile'), '"', '') AS director_ktp_file
  ) AS company_document,
  REPLACE(JSON_EXTRACT(A.data, '$.tracking'), '"', '') AS tracking,
  E.visibility,
  F.control,
  B.partner_ids,
  C.allowed_payment_type,
  CAST(JSON_EXTRACT(A.data, '$.isActive') AS BOOLEAN) AS is_active,
  CAST(JSON_EXTRACT(A.data, '$.isDeleted') AS BOOLEAN) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.salesId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.salesId'), '"', '')) AS sales_id,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.salesName'), '"', '') = "-", NULL, REPLACE(JSON_EXTRACT(A.data, '$.salesName'), '"', '')) AS sales_name,
  A.data AS original_data,
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