WITH
  base AS (
  SELECT
    *
  FROM
    `logee-data-prod.logee_datalake_raw_production.visibility_lgd_store`
  WHERE
    _date_partition >= '2022-01-01' )

-- BEGIN listAddress
,listAddress as ( SELECT 
data,
ts AS published_timestamp,
  ARRAY_AGG(
    STRUCT(
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.storeName'), '"', '') AS store_name,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.address'), '"', '')  AS address,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.addressId'), '"', '')  AS address_id,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.cityId'), '"', '') AS city_id,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.district'), '"', '') AS district,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.districtId'), '"', '') AS district_id,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.lat'), '"', '') AS FLOAT64) AS lat,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.long'), '"', '') AS FLOAT64) AS long,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.phoneNumber'), '"', '') AS phone_number,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.province'), '"', '') AS province,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.provinceId'), '"', '') AS province_id,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.subDistrict'), '"', '') AS sub_district,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.subDistrictId'), '"', '') AS sub_district_id,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.zipCode'), '"', '') AS zip_code,
    REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.zipCodeId'), '"', '') AS zip_code_id,
    IF(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(listAddress,'$.externalId'), '"', '')) AS external_id,
    IF(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(listAddress,'$.recipientName'), '"', '')) AS recipient_name,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.mainAddress'), '"', '') AS BOOL) AS main_address,
    IF(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.addressMark'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(listAddress,'$.addressMark'), '"', '')) AS address_mark,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(listAddress,'$.isFulfillmentProcess'), '"', '') AS BOOL) AS is_fulfillment_process
  )
  )AS list_address
  
  from base,
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.listAddress')) AS listAddress
  group by 1,2
)
 
-- END listAddress
,ownerAddress as (  
  SELECT 
  data,
    ts AS published_timestamp,
  struct(
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.address'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.address'), '"', '')) AS address,
       CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.storeOwner.ownerAddress.lat'), '"', '') AS FLOAT64) AS lat,
       CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.storeOwner.ownerAddress.long'), '"', '') AS FLOAT64) AS long,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.provinceId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.provinceId'), '"', '')) AS province_id,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.province'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.province'), '"', '')) AS province,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.cityId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.cityId'), '"', '')) AS city_id,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.city'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.city'), '"', '')) AS city,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.districtId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.districtId'), '"', '')) AS district_id,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.district'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.district'), '"', '')) AS district,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.subDistrictId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.subDistrictId'), '"', '')) AS sub_district_id,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.subDistrict'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.subDistrict'), '"', '')) AS sub_district,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.zipCodeId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.zipCodeId'), '"', '')) AS zip_code_id,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.zipCode'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.zipCode'), '"', '')) AS zip_code,
       IF(REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.addressMark'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.storeOwner.ownerAddress.addressMark'), '"', '')) AS address_mark
      ) as owner_address
FROM base
)

,storeOwner AS (
  SELECT
    A.data,
    ts AS published_timestamp,
    STRUCT(
      B.owner_address,
       REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.storeOwner.ownerName'), '"', '') AS owner_name,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.storeOwner.ownerPhoneNumber'), '"', '') AS owner_phone_number
    ) AS store_owner
  FROM base A
    LEFT JOIN ownerAddress B
    ON A.data = B.data
    AND A.ts = B.published_timestamp
)
-- storeOwner

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

-- end storeOwner

select
 REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.storeId'), '"', '') AS store_id,
B.list_Address,
  C.store_owner,
  JSON_EXTRACT_SCALAR(A.data, '$.userType') AS user_type,
  JSON_EXTRACT_SCALAR(A.data, '$.companyId') AS company_id,
  IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.username'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.username'), '"', '')) AS username,
  IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.externalId'), '"', '')) AS external_id,
  JSON_EXTRACT_SCALAR(A.data, '$.subAreaId') AS sub_area_id,
  JSON_EXTRACT_SCALAR(A.data, '$.salesId') AS sales_id,
  JSON_EXTRACT_SCALAR(A.data, '$.mainAddressId') AS main_address_id,
    STRUCT(
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.address'), '"', '')  AS address,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.cityId'), '"', '') AS city_id,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.district'), '"', '') AS district,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.districtId'), '"', '') AS district_id,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.lat'), '"', '') as FLOAT64) AS lat,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.long'), '"', '') as FLOAT64) AS long,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.phoneNumber'), '"', '') AS phone_number,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.province'), '"', '') AS province,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.provinceId'), '"', '') AS province_id,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.storeName'), '"', '') AS store_name,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.subDistrict'), '"', '') AS sub_district,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.subDistrictId'), '"', '') AS sub_district_id,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.zipCode'), '"', '') AS zip_code,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.zipCodeId'), '"', '') AS zip_code_id,
    IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.mainAddress.externalId'), '"', '')) AS external_id,
    IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.mainAddress.recipientName'), '"', '')) AS recipient_name,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.mainAddress'), '"', '') AS BOOL) AS main_address,
     IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.addressMark'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.mainAddress.addressMark'), '"', '')) AS address_mark,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.mainAddress.isFulfillmentProcess'), '"', '') AS BOOL) AS is_fulfillment_process
  ) AS main_address,
   REPLACE(JSON_EXTRACT(A.data, '$.status'), '"', '')AS status,
   IF(REPLACE(JSON_EXTRACT(A.data, '$.assignedTaskId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.assignedTaskId'), '"', '')) AS assigned_task_id,
  D.apps,
  REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '')AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.isActive'), '"', '') AS BOOL) AS is_active,
  
  A.data AS original_data,
  A.ts AS published_timestamp
from base A
LEFT JOIN listAddress B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN storeOwner C
  ON A.data = C.data
  AND A.ts = C.published_timestamp

   LEFT JOIN apps D
  ON A.data = D.data
  AND A.ts = D.published_timestamp
