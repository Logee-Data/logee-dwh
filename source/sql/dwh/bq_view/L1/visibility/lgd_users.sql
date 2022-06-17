WITH 

base AS (
  SELECT * FROM `logee-data-dev.logee_datalake_raw_development.visibility_lgd_users` 
  WHERE _date_partition >= "2022-01-01"
)

-- BEGIN APPS

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

-- END APPS

-- BEGIN ROLES

,roles AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(roles, '"', '')
    ) AS roles
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.roles')) AS roles
  GROUP BY 1,2
)

-- END ROLES

-- BEGIN STORE_IMAGES
,pre_store_images AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(store_images, '$.image') AS image,
      JSON_EXTRACT_SCALAR(store_images, '$.mainImage') AS main_image
    ) AS store_images
  FROM base,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.user_metadata'), '$.storeImages')) AS store_images
)

,store_images AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG(store_images) AS store_images
  FROM pre_store_images
  GROUP BY 1, 2
)

-- END STORE_IMAGES

-- BEGIN LIST_ADDRESS

,pre_list_address AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(list_address, '$.storeName') AS store_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.recipientName'), '"', ''))  AS recipient_name,
      JSON_EXTRACT_SCALAR(list_address, '$.phoneNumber') AS phone_number,
      JSON_EXTRACT_SCALAR(list_address, '$.province') AS province,
      JSON_EXTRACT_SCALAR(list_address, '$.provinceId') AS province_id,
      JSON_EXTRACT_SCALAR(list_address, '$.city') AS city,
      JSON_EXTRACT_SCALAR(list_address, '$.cityId') AS city_id,
      JSON_EXTRACT_SCALAR(list_address, '$.district') AS district,
      JSON_EXTRACT_SCALAR(list_address, '$.districtId') AS district_id,
      JSON_EXTRACT_SCALAR(list_address, '$.subDistrict') AS sub_district,
      JSON_EXTRACT_SCALAR(list_address, '$.subDistrictId') AS sub_district_id,
      JSON_EXTRACT_SCALAR(list_address, '$.zipCode') AS zip_code,
      JSON_EXTRACT_SCALAR(list_address, '$.zipCodeId') AS zipcode_id,
      JSON_EXTRACT_SCALAR(list_address, '$.address') AS address,
      JSON_EXTRACT_SCALAR(list_address, '$.mainAddress') AS main_address,
      IF(JSON_EXTRACT_SCALAR(list_address, '$.externalId') = "", NULL, JSON_EXTRACT(list_address, '$.externalId'))  AS external_id,
      JSON_EXTRACT_SCALAR(list_address, '$.lat') AS lat,
      JSON_EXTRACT_SCALAR(list_address, '$.long') AS long,
      CAST(JSON_EXTRACT_SCALAR(list_address, '$.isFulfillmentProcess') AS BOOL) AS is_fulfillment_process,
      JSON_EXTRACT_SCALAR(list_address, '$.addressId') AS address_id,
      IF(JSON_EXTRACT_SCALAR(list_address, '$.addressMark') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.addressMark')) AS address_mark
    ) AS list_address
  FROM base, UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.user_metadata'), '$.listAddress')) AS list_address
)

,list_address AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG(list_address) AS list_address
  FROM pre_list_address
  GROUP BY 1, 2
)

-- END LIST_ADDRESS

-- BEGIN METADATA
,metadata AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT (
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.metadata.firstOrderId'), '"', '') AS first_order_id
    ) AS metadata
  FROM base
)
-- END METADATA

-- BEGIN MAIN_ADDRESS
,main_address AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.storeName') AS store_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.recipientName'), '"', ''))  AS recipient_name,
      CAST(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.phoneNumber') AS INT64) AS phone_number,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.province') AS province,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.provinceId	') AS province_id,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.city') AS city,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.cityId') AS city_id,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.district') AS district,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.districtId') AS district_id,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.subDistrict') AS sub_district,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.subDistrictId') AS sub_district_id,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.zipCode') AS zip_code,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.zipCodeId') AS zipcode_id,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.address') AS address,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.mainAddress') AS main_address,
      IF(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.externalId') = "", NULL, JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.externalId')) AS external_id,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.lat') AS lat,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.long') AS long,
      CAST(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.isFulfillmentProcess') AS BOOL) AS is_fulfillment_process,
      JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.addressId') AS address_id,
      IF(JSON_EXTRACT_SCALAR(data, '$.user_metadata.mainAddress.addressMark') = "", NULL, JSON_EXTRACT(data, '$.user_metadata.mainAddress.addressMark')) AS address_mark
    ) AS main_address
  FROM base
)
-- END MAIN_ADDRESS

-- BEGIN STORE_OWNER
,store_owner AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT (
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerName'), '"', '') AS owner_name,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerPhoneNumber'), '"', '') AS INT64) AS owner_phone_number,
      STRUCT (
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.province'), '"', '') AS province,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.provinceId'), '"', '') AS province_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.city'), '"', '') AS city,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.cityId'), '"', '') AS city_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.district'), '"', '') AS district,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.districtId'), '"', '') AS district_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.subDistrict'), '"', '') AS sub_district,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.subDistrictId'), '"', '') AS sub_district_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.zipCode'), '"', '') AS zip_code,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.zipCodeId'), '"', '') AS zip_code_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.address'), '"', '') AS address,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.lat'), '"', '') AS lat,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.storeOwner.ownerAddress.long'), '"', '') AS long
      ) AS owner_address
    ) AS store_owner
  FROM
    base
)
-- END STORE_OWNER

-- BEGIN INTEGRATED_ACCOUNT
,integrated_account AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      STRUCT (
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.integratedAccount.tmoney.userName'), '"', '') AS user_name,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.integratedAccount.tmoney.phoneNo'), '"', '') AS INT64) AS phone_number,
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.integratedAccount.tmoney.signature'), '"', '') AS signature,
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.integratedAccount.tmoney.idTmoney'), '"', '') AS id_Tmoney,
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.integratedAccount.tmoney.idFusion'), '"', '') AS id_Fusion,
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.user_metadata.integratedAccount.tmoney.token'), '"', '') AS token,
      CAST(REPLACE(JSON_EXTRACT(data, '$.user_metadata.integratedAccount.tmoney.tokenExpiredAt'), '"', '') AS TIMESTAMP) AS token_expired_at
      ) AS tmoney
    ) AS integrated_account
  FROM
    base
)
-- END INTEGRATED_ACCOUNT

-- BEGIN user_metadata
,user_metadata AS (
  SELECT
    A.data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.userType') AS user_type,
      F.store_owner,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.salesId') AS sales_id,
      C.list_address,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.storeId') AS store_id,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.companyId') AS company_id,
      IF(JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.externalId') = "", NULL, JSON_EXTRACT(A.data, '$.user_metadata.externalId')) AS external_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.user_metadata.areaId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.user_metadata.areaId'), '"', '')) AS area_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.user_metadata.subAreaId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.user_metadata.subAreaId'), '"', '')) AS sub_area_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.user_metadata.mainAddressId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.user_metadata.mainAddressId'), '"', '')) AS main_address_id,
      D.main_address,
      IF(JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.assignedTaskId') = "", NULL, JSON_EXTRACT(A.data, '$.user_metadata.assignedTaskId'))  AS assigned_task_id,
      JSON_EXTRACT_ARRAY(A.data, '$.user_metadata.apps') AS apps,
      B.store_images,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.userId') AS user_id,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.storeCode') AS store_code,
      G.integrated_account,
      E.metadata,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.approvedAt') AS approved_at,
      JSON_EXTRACT_SCALAR(A.data, '$.user_metadata.salesEmployeeStatus') AS sales_employee_status,
      CAST(REPLACE(JSON_EXTRACT(A.data, '$.isActive'), '"', '') AS BOOL) AS is_active
    ) AS user_metadata
  
  FROM base A

  LEFT JOIN store_images B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN list_address C
  ON A.data = C.data
  AND A.ts = B.published_timestamp

  LEFT JOIN main_address D
  ON A.data = D.data
  AND A.ts = C.published_timestamp

  LEFT JOIN metadata E
  ON A.data = E.data
  AND A.ts = D.published_timestamp

  LEFT JOIN store_owner F
  ON A.data = F.data
  AND A.ts = E.published_timestamp

  LEFT JOIN integrated_account G
  ON A.data = G.data
  AND A.ts = E.published_timestamp
)

-- END user_metadata

-- BEGIN MAIN

SELECT
  REPLACE(JSON_EXTRACT(A.data, '$.userId'), '"', '') AS user_id,
  REPLACE(JSON_EXTRACT(A.data, '$.username'), '"', '') AS user_name,
  REPLACE(JSON_EXTRACT(A.data, '$.userType'), '"', '') AS user_type,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '')) AS email,
  REPLACE(JSON_EXTRACT(A.data, '$.phoneNumber'), '"', '') AS phone_number,
  REPLACE(JSON_EXTRACT(A.data, '$.roleId'), '"', '') AS role_id,
  REPLACE(JSON_EXTRACT(A.data, '$.loginCount'), '"', '') AS login_count,
  B.apps AS apps,
  C.roles AS roles,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.workArea'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.workArea'), '"', '')) AS work_area,
  D.user_metadata AS user_metadata,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isActive'), '"', '') AS BOOL) AS is_active,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  A.data,
  A.ts AS published_timestamp
  
  FROM base A
  LEFT JOIN apps B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN roles C
  ON A.data = C.data
  AND A.ts = C.published_timestamp

  LEFT JOIN user_metadata D
  ON A.data = D.data
  AND A.ts = D.published_timestamp

-- END MAIN