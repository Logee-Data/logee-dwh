WITH 
base AS (
  SELECT * FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_companies`
  WHERE _date_partition >= '2022-01-01'
  )

-- BEGIN companyPartnership

,partnershipCompanyId as ( SELECT 
data,
ts AS published_timestamp,
  ARRAY_AGG(
    STRUCT(
      IF(REPLACE(JSON_EXTRACT(company_partnership,'$.partnershipCompanyId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(company_partnership,'$.partnershipCompanyId'), '"', '')) AS partnership_company_id
    )
  )AS company_partnership
  from base,
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.companyPartnership')) AS company_partnership
  group by 1,2
)
-- END companyPartnership

-- BEGIN bankAccount
,bankAccount AS (
  SELECT
  data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(data, '$.bankAccount.bankName') AS `bank_name`,
      JSON_EXTRACT_SCALAR(data, '$.bankAccount.accountName') AS `account_name`,
      JSON_EXTRACT_SCALAR(data, '$.bankAccount.accountNumber') AS `account_number`
    ) AS bank_account
  FROM base 
)
-- END bankAccount

-- Settings
,purchaseOrder as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
   CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.settings.purchaseOrder.isActive'), '"', '') AS BOOL) AS `is_active`,
       CAST(JSON_EXTRACT_SCALAR(data, '$.settings.purchaseOrder.allowChanges')AS BOOL) AS `allow_changes`,
      IF(REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOrder.notes'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOrder.notes'), '"', '')) AS notes
      ) as purchase_order
FROM base
)

,salesOrder as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
    CAST(JSON_EXTRACT_SCALAR(data, '$.settings.salesOrder.isActive')AS BOOL) AS `is_active`,
      IF(REPLACE(JSON_EXTRACT(data, '$.settings.salesOrder.notes'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOrder.notes'), '"', '')) AS notes
      ) as sales_order
FROM base
)

,invoice as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
  CAST(JSON_EXTRACT_SCALAR(data, '$.settings.invoice.isActive')AS BOOL) AS `is_active`,
      IF(REPLACE(JSON_EXTRACT(data, '$.settings.invoice.notes'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOrder.notes'), '"', '')) AS notes,
      IF(REPLACE(JSON_EXTRACT(data, '$.settings.invoice.approverName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOrder.approverName'), '"', '')) AS approver_name,
     IF(REPLACE(JSON_EXTRACT(data, '$.settings.invoice.approverPosition'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOrder.approverPosition'), '"', '')) AS approver_position
  ) as invoice

FROM base
)

,purchaseOnDelivery as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
   CAST(JSON_EXTRACT_SCALAR(data, '$.settings.purchaseOnDelivery.isActive')AS BOOL) AS `is_active`,
   IF(REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOnDelivery.notes'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOnDelivery.notes'), '"', '')) AS notes,
   IF(REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOnDelivery.approverName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOnDelivery.approverName'), '"', '')) AS approver_name,
    IF(REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOnDelivery.approverPosition'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.settings.purchaseOnDelivery.approverPosition'), '"', '')) AS approver_position
      ) as purchase_on_delivery
FROM base
)

,payment as ( SELECT 
data,
ts AS published_timestamp,
  ARRAY_AGG(
    STRUCT(
      REPLACE(JSON_EXTRACT(payment,'$.paymentId'),'"', '') AS payment_id,
       CAST(JSON_EXTRACT(payment,'$.isActive')AS BOOL) AS is_active
    )
  )AS payment
  from base,
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.settings.payment')) AS payment
  group by 1,2
)

,services as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
   JSON_EXTRACT_SCALAR(data, '$.settings.services.order') AS `order`,
      JSON_EXTRACT_SCALAR(data, '$.settings.services.sales') AS `sales`
      ) as services
FROM base
)

,feature as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
    CAST(JSON_EXTRACT_SCALAR(data, '$.settings.feature.isOtp')AS BOOL) AS `is_otp`
      ) as feature
FROM base
)

,settings AS (
  SELECT
    A.data,
    ts AS published_timestamp,
    STRUCT(
      B.purchase_order,
      C.sales_order,
      D.invoice,
      E.purchase_on_delivery,
      F.payment,
      G.services,
      H.feature,
      JSON_EXTRACT_SCALAR(A.data, '$.settings.fulfillmentType') AS fulfillment_type,
      JSON_EXTRACT_SCALAR(A.data, '$.settings.isDefaultAreaSubArea') AS is_default_area_sub_area,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.settings.catalogueProductUrl'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.settings.catalogueProductUrl'), '"', '')) AS catalogue_product_url
      
    ) AS settings
  FROM base A
    LEFT JOIN purchaseOrder B
    ON A.data = B.data
    AND A.ts = B.published_timestamp

    LEFT JOIN salesOrder C
    ON A.data = C.data
    AND A.ts = C.published_timestamp

    LEFT JOIN invoice D
    ON A.data = D.data
    AND A.ts = D.published_timestamp

    LEFT JOIN purchaseOnDelivery E
    ON A.data = E.data
    AND A.ts = E.published_timestamp

    LEFT JOIN payment F
    ON A.data = F.data
    AND A.ts = F.published_timestamp

    LEFT JOIN services G
    ON A.data = G.data
    AND A.ts = G.published_timestamp

    LEFT JOIN feature H
    ON A.data = H.data
    AND A.ts = H.published_timestamp
)
-- end settings

-- APPS
,apps as (  
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(apps, '"', '')
    ) AS apps
  FROM base,
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.apps')) AS apps
  GROUP BY 1,2
)
-- END APPS

SELECT 
  JSON_EXTRACT_SCALAR(A.data, '$.companyName') AS company_name,
  JSON_EXTRACT_SCALAR(A.data, '$.companyPhoneNumber') AS company_phone_number,
  JSON_EXTRACT_SCALAR(A.data, '$.companyAddress') AS company_address,
  JSON_EXTRACT_SCALAR(A.data, '$.companyCategory') AS company_category,
  JSON_EXTRACT_SCALAR(A.data, '$.companyStatus') AS company_status,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.companyImage'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.companyImage'), '"', '')) AS company_image,
  JSON_EXTRACT_SCALAR(A.data, '$.userId') AS user_id,
  JSON_EXTRACT_SCALAR(A.data, '$.userType') AS user_type,
  B.company_partnership,
  C.bank_account,
  D.settings,
  F.apps,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.isActive')AS BOOL) AS is_active,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.isDeleted')AS BOOL) AS is_deleted,
 CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.companyGroupId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.companyGroupId'), '"', '')) AS company_group_id,
  A.data AS original_data,
  A.ts AS published_timestamp
  FROM base A
  LEFT JOIN partnershipCompanyId B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN bankAccount C
  ON A.data = C.data
  AND A.ts = C.published_timestamp 

  LEFT JOIN settings D
  ON A.data = D.data
  AND A.ts = D.published_timestamp

  LEFT JOIN apps F
  ON A.data = F.data
  AND A.ts = F.published_timestamp