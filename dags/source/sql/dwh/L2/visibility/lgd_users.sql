WITH base AS(
  SELECT 
  * 
  REPLACE(
    STRUCT (
        user_metadata.user_type,
        STRUCT (
          TO_HEX(SHA256(user_metadata.store_owner.owner_name)) AS owner_name,
          TO_HEX(SHA256(user_metadata.store_owner.owner_phone_number)) AS owner_phone_number,
          STRUCT (
            user_metadata.store_owner.owner_address.province,
            user_metadata.store_owner.owner_address.province_id,
            user_metadata.store_owner.owner_address.city,
            user_metadata.store_owner.owner_address.city_id,
            user_metadata.store_owner.owner_address.district,
            user_metadata.store_owner.owner_address.district_id,
            user_metadata.store_owner.owner_address.sub_district,
            user_metadata.store_owner.owner_address.sub_district_id,
            user_metadata.store_owner.owner_address.zip_code,
            user_metadata.store_owner.owner_address.zip_code_id,
            user_metadata.store_owner.owner_address.address,
            user_metadata.store_owner.owner_address.lat,
            user_metadata.store_owner.owner_address.long
          ) AS owner_address
        ) AS store_owner,
        user_metadata.sales_id,
        user_metadata.list_address,
        user_metadata.store_id,
        user_metadata.company_id,
        user_metadata.external_id,
        user_metadata.area_id,
        user_metadata.sub_area_id,
        user_metadata.main_address_id,
        STRUCT (
          TO_HEX(SHA256(user_metadata.main_address.store_name)) AS store_name,
          TO_HEX(SHA256(user_metadata.main_address.recipient_name)) AS recipient_name,
          TO_HEX(SHA256(user_metadata.main_address.phone_number)) AS phone_number,
          user_metadata.main_address.province,
          user_metadata.main_address.province_id,
          user_metadata.main_address.city,
          user_metadata.main_address.city_id,
          user_metadata.main_address.district,
          user_metadata.main_address.district_id,
          user_metadata.main_address.sub_district,
          user_metadata.main_address.sub_district_id,
          user_metadata.main_address.zip_code,
          user_metadata.main_address.zip_code_id,
          user_metadata.main_address.address,
          user_metadata.main_address.lat,
          user_metadata.main_address.long,
          user_metadata.main_address.is_fulfillment_process,
          user_metadata.main_address.address_id,
          user_metadata.main_address.address_mark
        ) AS main_address,
        user_metadata.assigned_task_id,
        user_metadata.apps,
        user_metadata.store_images,
        user_metadata.user_id,
        user_metadata.store_code,
        STRUCT (
          STRUCT (
          TO_HEX(SHA256(user_metadata.integrated_account.tmoney.user_name)) AS user_name,
          TO_HEX(SHA256(user_metadata.integrated_account.tmoney.phone_number)) AS phone_number,
          TO_HEX(SHA256(user_metadata.integrated_account.tmoney.signature)) AS signature,
          TO_HEX(SHA256(user_metadata.integrated_account.tmoney.id_tmoney)) AS id_tmoney,
          TO_HEX(SHA256(user_metadata.integrated_account.tmoney.id_fusion)) AS id_fusion,
          user_metadata.integrated_account.tmoney.token,
          user_metadata.integrated_account.tmoney.token_expired_at
          ) AS tmoney
        ) AS integrated_account,
        STRUCT (
          user_metadata.metadata.first_order_id
        ) AS metadata,
        user_metadata.approved_at,
        user_metadata.sales_employee_status,
        user_metadata.is_active
      ) AS user_metadata,
    TO_HEX(SHA256(user_name)) AS user_name,
    TO_HEX(SHA256(password)) AS password,
    TO_HEX(SHA256(email)) AS email,
    TO_HEX(SHA256(phone_number)) AS phone_number
  )
  FROM `logee-data-prod.L1_visibility.lgd_users`
  WHERE modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

,check AS (

  -- MAIN
  
  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'user_id' AS column,
      IF(user_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_id IS NULL or user_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'user_name' AS column,
      IF(user_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_name IS NULL or user_name = ''  

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'user_type' AS column,
      IF(user_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_type IS NULL or user_type = ''  

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'password' AS column,
      IF(password IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE password IS NULL or password = ''  

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'email' AS column,
      IF(email IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE email IS NULL or email = ''  

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'phone_number' AS column,
      IF(phone_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE phone_number IS NULL or phone_number = ''  

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'role_id' AS column,
      IF(role_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE role_id IS NULL or role_id = ''  

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'login_count' AS column,
      IF(login_count IS NULL, 'Column can not be NULL', IF(login_count < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM base
  WHERE login_count IS NULL or login_count < 0

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'work_area' AS column,
      IF(work_area IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE work_area IS NULL OR work_area = '' 

  -- USER_METADATA

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'user_type' AS column,
      IF(user_metadata.user_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.user_type IS NULL OR user_metadata.user_type = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sales_id' AS column,
      IF(user_metadata.sales_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.sales_id IS NULL OR user_metadata.sales_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'store_id' AS column,
      IF(user_metadata.store_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_id IS NULL OR user_metadata.store_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'company_id' AS column,
      IF(user_metadata.company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.company_id IS NULL OR user_metadata.company_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'external_id' AS column,
      IF(user_metadata.external_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.external_id IS NULL OR user_metadata.external_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'area_id' AS column,
      IF(user_metadata.area_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.area_id IS NULL OR user_metadata.area_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sub_area_id' AS column,
      IF(user_metadata.sub_area_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.sub_area_id IS NULL OR user_metadata.sub_area_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'main_address_id' AS column,
      IF(user_metadata.main_address_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address_id IS NULL OR user_metadata.main_address_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'assigned_task_id' AS column,
      IF(user_metadata.assigned_task_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.assigned_task_id IS NULL OR user_metadata.assigned_task_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'user_id' AS column,
      IF(user_metadata.user_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.user_id IS NULL OR user_metadata.user_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'store_code' AS column,
      IF(user_metadata.store_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_code IS NULL OR user_metadata.store_code = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sales_employee_status' AS column,
      IF(user_metadata.sales_employee_status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.sales_employee_status IS NULL OR user_metadata.sales_employee_status = ''

  -- USER_METADATA.STORE_OWNER

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'owner_name' AS column,
      IF(user_metadata.store_owner.owner_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_name IS NULL OR user_metadata.store_owner.owner_name = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'owner_phone_number' AS column,
      IF(user_metadata.store_owner.owner_phone_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_phone_number IS NULL OR user_metadata.store_owner.owner_phone_number = ''

  -- USER_METADATA.STORE_OWNER.OWNER_ADDRESS

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'province' AS column,
      IF(user_metadata.store_owner.owner_address.province IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.province IS NULL OR user_metadata.store_owner.owner_address.province = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'province_id' AS column,
      IF(user_metadata.store_owner.owner_address.province_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.province_id IS NULL OR user_metadata.store_owner.owner_address.province_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'city' AS column,
      IF(user_metadata.store_owner.owner_address.city IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.city IS NULL OR user_metadata.store_owner.owner_address.city = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'city_id' AS column,
      IF(user_metadata.store_owner.owner_address.city_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.city_id IS NULL OR user_metadata.store_owner.owner_address.city_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'district' AS column,
      IF(user_metadata.store_owner.owner_address.district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.district IS NULL OR user_metadata.store_owner.owner_address.district = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'district_id' AS column,
      IF(user_metadata.store_owner.owner_address.district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.district_id IS NULL OR user_metadata.store_owner.owner_address.district_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sub_district' AS column,
      IF(user_metadata.store_owner.owner_address.sub_district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.sub_district IS NULL OR user_metadata.store_owner.owner_address.sub_district = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sub_district_id' AS column,
      IF(user_metadata.store_owner.owner_address.sub_district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.sub_district_id IS NULL OR user_metadata.store_owner.owner_address.sub_district_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'zip_code' AS column,
      IF(user_metadata.store_owner.owner_address.zip_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.zip_code IS NULL OR user_metadata.store_owner.owner_address.zip_code = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'zip_code_id' AS column,
      IF(user_metadata.store_owner.owner_address.zip_code_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.zip_code_id IS NULL OR user_metadata.store_owner.owner_address.zip_code_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'address' AS column,
      IF(user_metadata.store_owner.owner_address.address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.address IS NULL OR user_metadata.store_owner.owner_address.address = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'lat' AS column,
      IF(user_metadata.store_owner.owner_address.lat IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.lat IS NULL

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'long' AS column,
      IF(user_metadata.store_owner.owner_address.long IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.store_owner.owner_address.long IS NULL

  -- USER_METADATA.MAIN_ADDRESS
  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'store_name' AS column,
      IF(user_metadata.main_address.store_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.store_name IS NULL OR user_metadata.main_address.store_name = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'recipient_name' AS column,
      IF(user_metadata.main_address.recipient_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.recipient_name IS NULL OR user_metadata.main_address.recipient_name = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'phone_number' AS column,
      IF(user_metadata.main_address.phone_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.phone_number IS NULL OR user_metadata.main_address.phone_number = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'province' AS column,
      IF(user_metadata.main_address.province IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.province IS NULL OR user_metadata.main_address.province = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'province_id' AS column,
      IF(user_metadata.main_address.province_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.province_id IS NULL OR user_metadata.main_address.province_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'city' AS column,
      IF(user_metadata.main_address.city IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.city IS NULL OR user_metadata.main_address.city = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'city_id' AS column,
      IF(user_metadata.main_address.city_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.city_id IS NULL OR user_metadata.main_address.city_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'district' AS column,
      IF(user_metadata.main_address.district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.district IS NULL OR user_metadata.main_address.district = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'district_id' AS column,
      IF(user_metadata.main_address.district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.district_id IS NULL OR user_metadata.main_address.district_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sub_district' AS column,
      IF(user_metadata.main_address.sub_district IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.sub_district IS NULL OR user_metadata.main_address.sub_district = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'sub_district_id' AS column,
      IF(user_metadata.main_address.sub_district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.sub_district_id IS NULL OR user_metadata.main_address.sub_district_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'zip_code' AS column,
      IF(user_metadata.main_address.zip_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.zip_code IS NULL OR user_metadata.main_address.zip_code = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'zip_code_id' AS column,
      IF(user_metadata.main_address.zip_code_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.zip_code_id IS NULL OR user_metadata.main_address.zip_code_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'address' AS column,
      IF(user_metadata.main_address.address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.address IS NULL OR user_metadata.main_address.address = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'lat' AS column,
      IF(user_metadata.main_address.lat IS NULL, 'Column can not be NULL', 
        IF(user_metadata.main_address.lat = 0, 'Column can not be equal to zero', "Column can not be a negative number")
      ) AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.lat IS NULL or user_metadata.main_address.lat = 0

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'long' AS column,
      IF(user_metadata.main_address.long IS NULL, 'Column can not be NULL', 
        IF(user_metadata.main_address.long = 0, 'Column can not be equal to zero', "Column can not be a negative number")
      ) AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.long IS NULL or user_metadata.main_address.long <= 0

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'is_fulfillment_process' AS column,
      IF(user_metadata.main_address.is_fulfillment_process IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.is_fulfillment_process IS NULL

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'address_id' AS column,
      IF(user_metadata.main_address.address_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.address_id IS NULL OR user_metadata.main_address.address_id = ''

  UNION ALL

  SELECT
    user_id,
    published_timestamp,

    STRUCT(
      'address_mark' AS column,
      IF(user_metadata.main_address.address_mark IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM base
  WHERE user_metadata.main_address.address_mark IS NULL OR user_metadata.main_address.address_mark = ''
)

,aggregated_check AS (
  SELECT
    user_id,
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
  ON A.user_id = B.user_id
  AND A.published_timestamp = B.published_timestamp