WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.L2_visibility.lgd_store`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

,first_explode AS (
  SELECT
    store_id,
    STRUCT (
      list_Address.store_name AS store_name,
      list_Address.address AS address,
      list_Address.address_id AS address_id,
      list_Address.city AS city,
      list_Address.city_id AS city_id,
      list_Address.district AS district,
      list_Address.district_id AS district_id,
      list_Address.lat AS lat,
      list_Address.long AS long,
      list_Address.phone_number AS phone_number,
      list_Address.province AS province,
      list_Address.province_id AS province_id,
      list_Address.sub_district AS sub_district,
      list_Address.sub_district_id AS sub_district_id,
      list_Address.zip_code AS zip_code,
      list_Address.zip_code_id AS zip_code_id,
      list_Address.external_id AS external_id,
      list_Address.recipient_name AS recipient_name,
      list_Address.main_address AS main_address,
      list_Address.address_mark AS address_mark,
      list_Address.is_fulfillment_process AS is_fulfillment_process
    ) AS list_Address,
    modified_at,
    created_at,
    published_timestamp
  FROM
    base,
    UNNEST(list_Address) AS list_Address
)

,lgd_store_list_Address AS (
  SELECT
     store_id,
      list_Address.store_name AS store_name,
      list_Address.address AS address,
      list_Address.address_id AS address_id,
      list_Address.city AS city,
      list_Address.city_id AS city_id,
      list_Address.district AS district,
      list_Address.district_id AS district_id,
      list_Address.lat AS lat,
      list_Address.long AS long,
      list_Address.phone_number AS phone_number,
      list_Address.province AS province,
      list_Address.province_id AS province_id,
      list_Address.sub_district AS sub_district,
      list_Address.sub_district_id AS sub_district_id,
      list_Address.zip_code AS zip_code,
      list_Address.zip_code_id AS zip_code_id,
      list_Address.external_id AS external_id,
      list_Address.recipient_name AS recipient_name,
      list_Address.main_address AS main_address,
      list_Address.address_mark AS address_mark,
      list_Address.is_fulfillment_process AS is_fulfillment_process,
    modified_at,
    created_at,
    published_timestamp
  FROM
    first_explode
)
select
*
FROM lgd_store_list_Address

