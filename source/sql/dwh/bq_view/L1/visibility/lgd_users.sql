WITH 

base AS (
  SELECT * FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_users` 
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
      IF(REPLACE(JSON_EXTRACT_SCALAR(store_images, '$.image'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(store_images, '$.image'), '"', ''))  AS image,
      IF(REPLACE(JSON_EXTRACT_SCALAR(store_images, '$.mainImage'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(store_images, '$.mainImage'), '"', ''))  AS main_image
    ) AS store_images
  FROM base,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.usermetadata'), '$.storeImages')) AS store_images
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
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.storeName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.storeName'), '"', ''))  AS store_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.recipientName'), '"', ''))  AS recipient_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.phoneNumber'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.phoneNumber'), '"', ''))  AS phone_number,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.province'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.province'), '"', ''))  AS province,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.provinceId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.provinceId'), '"', ''))  AS province_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.city'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.city'), '"', ''))  AS city,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.cityId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.cityId'), '"', ''))  AS city_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.district'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.district'), '"', ''))  AS district,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.districtId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.districtId'), '"', ''))  AS district_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.subDistrict'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.subDistrict'), '"', ''))  AS sub_district,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.subDistrictId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.subDistrictId'), '"', ''))  AS sub_district_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.zipCode'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.zipCode'), '"', ''))  AS zip_code,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.zipCodeId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.zipCodeId'), '"', ''))  AS zipcode_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.address'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.address'), '"', ''))  AS address,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.mainAddress'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.mainAddress'), '"', ''))  AS main_address,
      IF(JSON_EXTRACT_SCALAR(list_address, '$.externalId') = "", NULL, JSON_EXTRACT(list_address, '$.externalId'))  AS external_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.lat'), '"', '') = "", NULL, CAST(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.lat'), '"', '') AS FLOAT64)) AS lat,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.long'), '"', '') = "", NULL, CAST(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.long'), '"', '') AS FLOAT64)) AS long,
      CAST(JSON_EXTRACT_SCALAR(list_address, '$.isFulfillmentProcess') AS BOOL) AS is_fulfillment_process,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.addressId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.addressId'), '"', ''))  AS address_id,
      IF(JSON_EXTRACT_SCALAR(list_address, '$.addressMark') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.addressMark')) AS address_mark
    ) AS list_address
  FROM base, UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.usermetadata'), '$.listAddress')) AS list_address
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
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.metadata.firstOrderId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.metadata.firstOrderId'), '"', ''))  AS first_order_id
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
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.storeName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.storeName'), '"', ''))  AS store_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.recipientName'), '"', ''))  AS recipient_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.phoneNumber'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.phoneNumber'), '"', ''))  AS phone_number,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.province'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.province'), '"', ''))  AS province,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.provinceId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.provinceId'), '"', ''))  AS province_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.city'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.city'), '"', ''))  AS city,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.cityId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.cityId'), '"', ''))  AS city_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.district'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.district'), '"', ''))  AS district,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.districtId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.districtId'), '"', ''))  AS district_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.subDistrict'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.subDistrict'), '"', ''))  AS sub_district,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.subDistrictId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.subDistrictId'), '"', ''))  AS sub_district_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.zipCode'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.zipCode'), '"', ''))  AS zip_code,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.zipCodeId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.zipCodeId'), '"', ''))  AS zip_code_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.address'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.address'), '"', ''))  AS address,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.lat'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.lat'), '"', ''))  AS lat,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.long'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.long'), '"', ''))  AS long,
      CAST(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.isFulfillmentProcess') AS BOOL) AS is_fulfillment_process,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.addressId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.addressId'), '"', ''))  AS address_id,
      IF(JSON_EXTRACT_SCALAR(data, '$.usermetadata.mainAddress.addressMark') = "", NULL, JSON_EXTRACT(data, '$.usermetadata.mainAddress.addressMark')) AS address_mark
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
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerName'), '"', ''))  AS owner_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerPhoneNumber'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerPhoneNumber'), '"', ''))  AS owner_phone_number,
      STRUCT (
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.province'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.province'), '"', ''))  AS province,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.provinceId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.provinceId'), '"', ''))  AS province_id,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.city'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.city'), '"', ''))  AS city,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.cityId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.cityId'), '"', ''))  AS city_id,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.district'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.district'), '"', ''))  AS district,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.districtId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.districtId'), '"', ''))  AS district_id,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.subDistrict'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.subDistrict'), '"', ''))  AS sub_district,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.subDistrictId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.subDistrictId'), '"', ''))  AS sub_district_id,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.zipCode'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.zipCode'), '"', ''))  AS zip_code,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.zipCodeId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.zipCodeId'), '"', ''))  AS zip_code_id,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.address'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.address'), '"', ''))  AS address,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.lat'), '"', '') = "", NULL, CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.lat'), '"', '') AS FLOAT64)) AS lat,
        IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.long'), '"', '') = "", NULL, CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.storeOwner.ownerAddress.long'), '"', '') AS FLOAT64)) AS long
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
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.userName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.userName'), '"', ''))  AS user_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.phoneNo'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.phoneNo'), '"', ''))  AS phone_number,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.signature'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.signature'), '"', ''))  AS signature,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.idTmoney'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.idTmoney'), '"', ''))  AS id_tmoney,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.idFusion'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.idFusion'), '"', ''))  AS id_fusion,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.token'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.usermetadata.integratedAccount.tmoney.token'), '"', ''))  AS token,
      CAST(REPLACE(JSON_EXTRACT(data, '$.usermetadata.integratedAccount.tmoney.tokenExpiredAt'), '"', '') AS TIMESTAMP) AS token_expired_at
      ) AS tmoney
    ) AS integrated_account
  FROM
    base
)
-- END INTEGRATED_ACCOUNT

-- BEGIN USERMETADATA
,usermetadata AS (
  SELECT
    A.data,
    ts AS published_timestamp,
    STRUCT(
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.userType'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.userType'), '"', '')) AS user_type,
      F.store_owner,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.salesId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.salesId'), '"', '')) AS sales_id,
      C.list_address,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.storeId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.storeId'), '"', '')) AS store_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.companyId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.companyId'), '"', '')) AS company_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.externalId'), '"', '')) AS external_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.areaId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.areaId'), '"', '')) AS area_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.subAreaId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.subAreaId'), '"', '')) AS sub_area_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.mainAddressId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.mainAddressId'), '"', '')) AS main_address_id,
      D.main_address,
      IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.usermetadata.assignedTaskId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.usermetadata.assignedTaskId'), '"', ''))  AS assigned_task_id,
      JSON_EXTRACT_ARRAY(A.data, '$.usermetadata.apps') AS apps,
      B.store_images,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.userId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.userId'), '"', '')) AS user_id,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.storeCode'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.storeCode'), '"', '')) AS store_code,
      G.integrated_account,
      E.metadata,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.approvedAt'), '"', '') = "", NULL, CAST(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.approvedAt'), '"', '') AS TIMESTAMP)) AS approved_at,
      IF(REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.salesEmployeeStatus'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.usermetadata.salesEmployeeStatus'), '"', '')) AS sales_employee_status,
      CAST(REPLACE(JSON_EXTRACT(A.data, '$.isActive'), '"', '') AS BOOL) AS is_active
    ) AS usermetadata
  
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

-- END USERMETADATA

-- BEGIN MAIN

SELECT
  IF(REPLACE(JSON_EXTRACT(A.data, '$.userId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.userId'), '"', '')) AS user_id,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.username'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.username'), '"', '')) AS user_name,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.userType'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.userType'), '"', '')) AS user_type,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.password'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.password'), '"', '')) AS password,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.email'), '"', '')) AS email,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.phoneNumber'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.phoneNumber'), '"', '')) AS phone_number,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.roleId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.roleId'), '"', '')) AS role_id,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.loginCount'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.loginCount'), '"', '')) AS login_count,
  B.apps AS apps,
  C.roles AS roles,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.workArea'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.workArea'), '"', '')) AS work_area,
  D.usermetadata AS user_metadata,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isActive'), '"', '') AS BOOL) AS is_active,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '')) AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '')) AS modified_by,
  A.data AS original_data,
  A.ts AS published_timestamp
  
  FROM base A
  LEFT JOIN apps B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN roles C
  ON A.data = C.data
  AND A.ts = C.published_timestamp

  LEFT JOIN usermetadata D
  ON A.data = D.data
  AND A.ts = D.published_timestamp

-- END MAIN