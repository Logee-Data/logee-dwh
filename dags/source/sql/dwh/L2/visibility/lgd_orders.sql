WITH base AS (
  SELECT
    *
    REPLACE (
      STRUCT (
        store._id,
        store.store_id,
        store.approved_at,
        store.apps,
        store.area_id,
        store.assigned_task_id,
        store.company_id,
        store.created_at,
        store.created_by,
        store.external_id,
        store.is_deleted,
        STRUCT (
          store.main_address.store_name,
          store.main_address.recipient_name,
          TO_HEX(SHA256(store.main_address.phone_number)) AS phone_number,
          store.main_address.province,
          store.main_address.province_id,
          store.main_address.city,
          store.main_address.city_id,
          store.main_address.district,
          store.main_address.district_id,
          store.main_address.sub_district,
          store.main_address.sub_district_id,
          store.main_address.zip_code,
          store.main_address.zipcode_id,
          store.main_address.address,
          store.main_address.main_address,
          store.main_address.external_id,
          store.main_address.lat,
          store.main_address.long,
          store.main_address.is_fulfillment_process,
          store.main_address.address_id
        ) AS main_address,
        store.main_address_id,
        store.modified_at,
        store.modified_by,
        store.sales_employee_status,
        store.sales_id,
        store.status,
        store.store_code,
        STRUCT (
          store.store_owner.owner_name,
          TO_HEX(SHA256(store.store_owner.owner_phone_number)) AS owner_phone_number,
          store.store_owner.owner_address
        ) AS store_owner,
        store.sub_area_id,
        store.user_id,
        store.user_type,
        store.metadata,
        store.is_active,
        TO_HEX(SHA256(store.username)) AS username
      ) AS store,
      STRUCT (
          order_address.store_name,
          order_address.recipient_name,
          TO_HEX(SHA256(order_address.phone_number)) AS phone_number,
          order_address.province,
          order_address.province_id,
          order_address.city,
          order_address.city_id,
          order_address.district,
          order_address.district_id,
          order_address.sub_district,
          order_address.sub_district_id,
          order_address.zip_code,
          order_address.zip_code_id,
          order_address.address,
          order_address.main_address,
          order_address.external_id,
          order_address.lat,
          order_address.long,
          order_address.is_fulfillment_process,
          order_address.address_id,
          order_address.address_mark
        ) AS order_address
    )
  FROM
    `logee-data-prod.L1_visibility.lgd_orders`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- BEGIN CHECK
,check AS (
  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      'order_id' AS column,
      IF(order_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    order_id IS NULL or order_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      'store_id' AS column,
      IF(store_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    store_id IS NULL or store_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      'url_purchase_on_delivery_pdf' AS column,
      IF(url_purchase_on_delivery_pdf IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    url_purchase_on_delivery_pdf IS NULL or url_purchase_on_delivery_pdf = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      'url_invoice_pdf' AS column,
      IF(url_invoice_pdf IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    url_invoice_pdf IS NULL or url_invoice_pdf = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      'order_discount' AS column,
      IF(order_discount IS NULL, 'Column can not be NULL', IF(order_discount < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_discount IS NULL or order_discount < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "store_area_id" AS column,
      IF(store.area_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    store.area_id IS NULL or store.area_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "store_assigned_task_id" AS column,
      IF(store.assigned_task_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    store.assigned_task_id IS NULL or store.assigned_task_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "store_username" AS column,
      IF(store.username IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    store.username IS NULL or store.username = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "store_main_address_province_id" AS column,
      IF(store.main_address.province_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    store.main_address.province_id IS NULL or store.main_address.province_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "store_main_address_address_id" AS column,
      IF(store.main_address.address_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    store.main_address.address_id IS NULL or store.main_address.address_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_price" AS column,
      IF(order_summary.total_price IS NULL, 'Column can not be NULL', IF(order_summary.total_price < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_price IS NULL or order_summary.total_price < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_discount" AS column,
      IF(order_summary.total_discount IS NULL, 'Column can not be NULL', IF(order_summary.total_discount < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_discount IS NULL or order_summary.total_discount < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_voucher_amount" AS column,
      IF(order_summary.total_voucher_amount IS NULL, 'Column can not be NULL', IF(order_summary.total_voucher_amount < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_voucher_amount IS NULL or order_summary.total_voucher_amount < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_used_voucher_amount" AS column,
      IF(order_summary.total_used_voucher_amount IS NULL, 'Column can not be NULL', IF(order_summary.total_used_voucher_amount < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_used_voucher_amount IS NULL or order_summary.total_used_voucher_amount < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_product_discount" AS column,
      IF(order_summary.total_product_discount IS NULL, 'Column can not be NULL', IF(order_summary.total_product_discount < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_product_discount IS NULL or order_summary.total_product_discount < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_inpoin_coin" AS column,
      IF(order_summary.total_inpoin_coin IS NULL, 'Column can not be NULL', IF(order_summary.total_inpoin_coin < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_inpoin_coin IS NULL or order_summary.total_inpoin_coin < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_additional_discount" AS column,
      IF(order_summary.total_additional_discount IS NULL, 'Column can not be NULL', IF(order_summary.total_additional_discount < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_additional_discount IS NULL or order_summary.total_additional_discount < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_additional_fee" AS column,
      IF(order_summary.total_additional_fee IS NULL, 'Column can not be NULL', IF(order_summary.total_additional_fee < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_additional_fee IS NULL or order_summary.total_additional_fee < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_additional_charge" AS column,
      IF(order_summary.total_additional_charge IS NULL, 'Column can not be NULL', IF(order_summary.total_additional_charge < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_additional_charge IS NULL or order_summary.total_additional_charge < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_fee" AS column,
      IF(order_summary.total_fee IS NULL, 'Column can not be NULL', IF(order_summary.total_fee < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_fee IS NULL or order_summary.total_fee < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "order_summary_total_payment" AS column,
      IF(order_summary.total_payment IS NULL, 'Column can not be NULL', IF(order_summary.total_payment < 0, 'Column can not be a negative number', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE order_summary.total_payment IS NULL or order_summary.total_payment < 0

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "payment_company_id" AS column,
      IF(payment.company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    payment.company_id IS NULL or payment.company_id = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "payment_payment_label" AS column,
      IF(payment.payment_label IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    payment.payment_label IS NULL or payment.payment_label = ''

  UNION ALL

  SELECT
    order_id,
    published_timestamp,
    STRUCT(
      "payment_payment_image" AS column,
      IF(payment.payment_image IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    payment.payment_image IS NULL or payment.payment_image = ''
)
-- END CHECK

,aggregated_check AS (
  SELECT
    order_id,
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
  ON A.order_id = B.order_id
  AND A.published_timestamp = B.published_timestamp