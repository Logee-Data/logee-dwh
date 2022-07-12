-- CHECK

WITH base as(
   SELECT 
    * 
    REPLACE(
    TO_HEX(SHA256(company_phone_number)) AS company_phone_number,
    TO_HEX(SHA256(company_address)) AS company_address,
    STRUCT(
      TO_HEX(SHA256(bank_account.account_name)) AS account_name,
      TO_HEX(SHA256(bank_account.account_number)) AS account_number,
      bank_account.bank_name AS bank_name
    ) AS bank_account
    )
  FROM `logee-data-prod.L1_visibility.lgd_companies`
  WHERE
     modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)


-- ,check AS (

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_id' AS column,
--       IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_id IS NULL or company_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_name' AS column,
--       IF(company_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE company_name IS NULL or company_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_phone_number' AS column,
--       IF(company_phone_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_phone_number IS NULL or company_phone_number = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_address' AS column,
--       IF(company_address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_address IS NULL or company_address = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_category' AS column,
--       IF(company_category IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE company_category IS NULL or company_category = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_status' AS column,
--       IF(company_status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_status IS NULL or company_status = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'company_image' AS column,
--       IF(company_image IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE company_image IS NULL or company_image = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'user_id' AS column,
--       IF(user_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE user_id IS NULL or user_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'user_type' AS column,
--       IF(user_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE user_type IS NULL or user_type = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_account_bank_name' AS column,
--       IF(bank_account.bank_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_account.bank_name IS NULL or bank_account.bank_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_account_account_name' AS column,
--       IF(bank_account.account_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_account.account_name IS NULL or bank_account.account_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_account_account_number' AS column,
--       IF(bank_account.account_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_account.account_number IS NULL or bank_account.account_number = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'purchase_order_is_active' AS column,
--       IF(settings.purchase_order.is_active IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.purchase_order.is_active IS NULL 

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'purchase_order.allow_changes' AS column,
--       IF(settings.purchase_order.allow_changes IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.purchase_order.allow_changes IS NULL 

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'sales_order_is_active' AS column,
--       IF(settings.sales_order.is_active IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.sales_order.is_active IS NULL 

--   UNION ALL

--    SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'invoice_is_active' AS column,
--       IF(settings.invoice.is_active IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.invoice.is_active IS NULL 

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'invoice_approver_name' AS column,
--       IF(settings.invoice.approver_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.invoice.approver_name IS NULL or settings.invoice.approver_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'invoice_approver_position' AS column,
--       IF(settings.invoice.approver_position IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.invoice.approver_position IS NULL or settings.invoice.approver_position = ''
  
--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'purchase_on_delivery_is_active' AS column,
--       IF(settings.purchase_on_delivery.is_active IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.purchase_on_delivery.is_active IS NULL 

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'purchase_on_delivery_approver_name' AS column,
--       IF(settings.purchase_on_delivery.approver_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.purchase_on_delivery.approver_name IS NULL or settings.purchase_on_delivery.approver_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'purchase_on_delivery_approver_position' AS column,
--       IF(settings.purchase_on_delivery.approver_position IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.purchase_on_delivery.approver_position IS NULL or settings.purchase_on_delivery.approver_position = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'services_order' AS column,
--       IF(settings.services.order IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.services.order IS NULL or settings.services.order = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'services_sales' AS column,
--       IF(settings.services.sales IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.services.sales IS NULL or settings.services.sales = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'feature_is_otp' AS column,
--       IF(settings.feature.is_otp IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.feature.is_otp IS NULL 

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'fulfillment_type' AS column,
--       IF(settings.fulfillment_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.fulfillment_type IS NULL or settings.fulfillment_type = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'is_default_area_sub_area' AS column,
--       IF(settings.is_default_area_sub_area IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE settings.is_default_area_sub_area IS NULL

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,
--     STRUCT(
--       'is_active' AS column,
--       IF(is_active IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE is_active IS NULL

--   UNION ALL

--  SELECT
--     company_id,
--     published_timestamp,
--     STRUCT(
--       'company_group_id' AS column,
--       IF(company_group_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_group_id IS NULL or company_group_id = ''

-- )

-- ,aggregated_check AS (
--   SELECT 
--     company_id,
--     published_timestamp,
--     ARRAY_AGG(
--       quality_check
--     ) AS quality_check
--   FROM check
--   GROUP BY 1, 2
-- )

SELECT
  *
  -- B.quality_check
FROM 
  base 
  -- LEFT JOIN aggregated_check B
  -- ON A.company_id = B.company_id
  -- AND A.published_timestamp = B.published_timestamp
