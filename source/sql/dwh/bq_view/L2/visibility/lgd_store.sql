-- CHECK

WITH base as(
   SELECT 
    * 
    REPLACE(
      TO_HEX(SHA256(username)) AS username,
      STRUCT(
        STRUCT(
        TO_HEX(SHA256(store_owner.owner_address.address)) AS address,
        store_owner.owner_address.lat AS lat,
        store_owner.owner_address.long AS long,
        store_owner.owner_address.province_id AS province_id,
        store_owner.owner_address.province AS province,
        store_owner.owner_address.city_id AS city_id,
        store_owner.owner_address.city AS city,
        store_owner.owner_address.district_id AS district_id,
        store_owner.owner_address.district AS district,
        store_owner.owner_address.sub_district_id AS sub_district_id,
        store_owner.owner_address.sub_district AS sub_district,
        store_owner.owner_address.zip_code_id AS zip_code_id,
        store_owner.owner_address.zip_code AS zip_code,
        store_owner.owner_address.address_mark AS address_mark
      ) AS owner_address,
      store_owner.owner_name AS owner_name,
      TO_HEX(SHA256(store_owner.owner_phone_number)) AS owner_phone_number
      )AS store_owner,
      STRUCT(
        main_address.store_name AS store_name,
        TO_HEX(SHA256(main_address.address)) AS address,
        main_address.lat AS lat,
        main_address.long AS long,
        main_address.province_id AS province_id,
        main_address.province AS province,
        main_address.city_id AS city_id,
        main_address.city AS city,
        main_address.district_id AS district_id,
        main_address.district AS district,
        main_address.sub_district_id AS sub_district_id,
        main_address.sub_district AS sub_district,
        main_address.zip_code_id AS zip_code_id,
        main_address.zip_code AS zip_code,
        main_address.external_id AS external_id,
        main_address.recipient_name AS recipient_name,
        main_address.main_address AS main_address,
        main_address.address_mark AS address_mark,
        main_address.is_fulfillment_process AS is_fulfillment_process,
        TO_HEX(SHA256(main_address.phone_number)) AS phone_number
      ) AS main_address
    )
  FROM `logee-data-prod.L1_visibility_playground.lgd_store`
)

,check AS (

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'store_id' AS column,
      IF(store_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_id IS NULL or store_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_address' AS column,
      IF(store_owner.owner_address.address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.address IS NULL or store_owner.owner_address.address = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_lat' AS column,
      IF(store_owner.owner_address.lat IS NULL, 'Column can not be NULL', 'Column can not be equal to zero') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.lat IS NULL or store_owner.owner_address.lat = 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_long' AS column,
      IF(store_owner.owner_address.long IS NULL, 'Column can not be NULL', 'Column can not be equal to zero') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.long IS NULL or store_owner.owner_address.long = 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_province_id' AS column,
      IF(store_owner.owner_address.province_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
 FROM base
  WHERE store_owner.owner_address.province_id IS NULL or store_owner.owner_address.province_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_province' AS column,
      IF(store_owner.owner_address.province IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.province IS NULL or store_owner.owner_address.province = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_city_id' AS column,
      IF(store_owner.owner_address.city_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
 FROM base
  WHERE store_owner.owner_address.city_id IS NULL or store_owner.owner_address.city_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_city' AS column,
      IF(store_owner.owner_address.city IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.city IS NULL or store_owner.owner_address.city = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_district_id' AS column,
      IF(store_owner.owner_address.district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
 FROM base
  WHERE store_owner.owner_address.district_id IS NULL or store_owner.owner_address.district_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_district' AS column,
      IF(store_owner.owner_address.district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.district IS NULL or store_owner.owner_address.district = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_sub_district_id' AS column,
      IF(store_owner.owner_address.sub_district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.sub_district_id IS NULL or store_owner.owner_address.sub_district_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_sub_district' AS column,
      IF(store_owner.owner_address.sub_district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.sub_district IS NULL or store_owner.owner_address.sub_district = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_zip_code_id' AS column,
      IF(store_owner.owner_address.zip_code_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.zip_code_id IS NULL or store_owner.owner_address.zip_code_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_zip_code' AS column,
      IF(store_owner.owner_address.zip_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.zip_code IS NULL or store_owner.owner_address.zip_code = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_address_address_mark' AS column,
      IF(store_owner.owner_address.address_mark IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_address.address_mark IS NULL or store_owner.owner_address.address_mark = ''

  UNION ALL

   SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_name' AS column,
      IF(store_owner.owner_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_name IS NULL or store_owner.owner_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'owner_phone_number' AS column,
      IF(store_owner.owner_phone_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE store_owner.owner_phone_number IS NULL or store_owner.owner_phone_number = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'user_type' AS column,
      IF(user_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_type IS NULL or user_type = ''
  
  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'company_id' AS column,
      IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE company_id IS NULL or company_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'username' AS column,
      IF(username IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE username IS NULL or username = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'external_id' AS column,
      IF(external_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE external_id IS NULL or external_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'sub_area_id' AS column,
      IF(sub_area_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sub_area_id IS NULL or sub_area_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'sales_id' AS column,
      IF(sales_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE sales_id IS NULL or sales_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_id' AS column,
      IF(main_address_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address_id IS NULL 

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_address' AS column,
      IF(main_address.address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.address IS NULL or main_address.address = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_city' AS column,
      IF(main_address.city IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.city IS NULL or main_address.city = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_city_id' AS column,
      IF(main_address.city_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.city_id IS NULL or main_address.city_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_district' AS column,
      IF(main_address.district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.district IS NULL or main_address.district = ''

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_district_id' AS column,
      IF(main_address.district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.district_id IS NULL or main_address.district_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_lat' AS column,
      IF(main_address.lat IS NULL, 'Column can not be NULL', 'Column can not be equal to zero') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.lat IS NULL or main_address.lat = 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_long' AS column,
      IF(main_address.long IS NULL, 'Column can not be NULL', 'Column can not be equal to zero') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.long IS NULL or main_address.long = 0

 UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_phone_number' AS column,
      IF(main_address.phone_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.phone_number IS NULL or main_address.phone_number = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_province' AS column,
      IF(main_address.province IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.province IS NULL or main_address.province = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_province_id' AS column,
      IF(main_address.province_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.province_id IS NULL or main_address.province_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,

    STRUCT(
      'main_address_store_name' AS column,
      IF(main_address.store_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.store_name IS NULL or main_address.store_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_sub_district' AS column,
      IF(main_address.sub_district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.sub_district IS NULL or main_address.sub_district = ''

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_sub_district_id' AS column,
      IF(main_address.sub_district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.sub_district_id is NULL or main_address.sub_district_id = ''


UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_zip_code' AS column,
      IF(main_address.zip_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.zip_code IS NULL or main_address.zip_code = ''

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_zip_code_id' AS column,
      IF(main_address.zip_code_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.zip_code_id is NULL or main_address.zip_code_id = ''

   UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_external_id' AS column,
      IF(main_address.external_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.external_id is NULL or main_address.external_id = ''

UNION ALL
  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_recipient_name' AS column,
      IF(main_address.recipient_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.recipient_name is NULL or main_address.recipient_name = ''

  UNION ALL
  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address' AS column,
      IF(main_address.main_address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.main_address is NULL 

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_address_mark' AS column,
      IF(main_address.address_mark IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.address_mark is NULL or main_address.address_mark = ''

UNION ALL
  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'main_address_is_fulfillment_process' AS column,
      IF(main_address.is_fulfillment_process IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE main_address.is_fulfillment_process is NULL

UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'status' AS column,
      IF(status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE status IS NULL or status = ''

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'assigned_task_id' AS column,
      IF(assigned_task_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE assigned_task_id is NULL or assigned_task_id = ''


UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_deleted' AS column,
      IF(is_deleted IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE is_deleted IS NULL 

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'is_active' AS column,
      IF(is_active IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE is_active IS NULL 
)

,aggregated_check AS (
  SELECT 
    original_data,
    published_timestamp,
    ARRAY_AGG(
      quality_check
    ) AS quality_check
  FROM check
  GROUP BY 1, 2
)

SELECT
  A.*,
  B.quality_check
FROM 
  base A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp