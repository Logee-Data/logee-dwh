-- CHECK

WITH base as(
    SELECT * 
        REPLACE(
            STRUCT (
            TO_HEX(SHA256(company_document.npwp_file)) AS npwp_file,
            TO_HEX(SHA256(company_document.siup_file)) AS siup_file,
            TO_HEX(SHA256(company_document.director_ktp_file)) AS director_ktp_file
            ) AS company_document,
            TO_HEX(SHA256(phone)) AS phone,
            TO_HEX(SHA256(email)) AS email,
            TO_HEX(SHA256(npwp_num)) AS npwp_num,
            TO_HEX(SHA256(siup_num)) AS siup_num,
            TO_HEX(SHA256(director_name)) AS director_name,
            TO_HEX(SHA256(director_ktp_num)) AS director_ktp_num,
            TO_HEX(SHA256(bank_account_name)) AS bank_account_name,
            TO_HEX(SHA256(bank_account_num)) AS bank_account_num,
            TO_HEX(SHA256(pks_num)) AS pks_num,
            TO_HEX(SHA256(mandiri_invoice_id)) AS mandiri_invoice_id,
            STRUCT (
                control.links,
                visibility.feature,
                TO_HEX(SHA256(visibility.document)) AS document
                ) AS visibility,
            
            STRUCT (
                control.links,
                control.feature,
                TO_HEX(SHA256(control.document)) AS document
                ) AS control
        )
    FROM 
        `logee-data-prod.L1_visibility.dma_logee_companies`
    WHERE
         modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- -- BEGIN CHECK
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
--       'name' AS column,
--       IF(name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE name IS NULL or name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'type' AS column,
--       IF(type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE type IS NULL or type = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'address' AS column,
--       IF(address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE address IS NULL or address = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'city_id' AS column,
--       IF(city_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE city_id IS NULL or city_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'city_name' AS column,
--       IF(city_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE city_name IS NULL or city_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'province_id' AS column,
--       IF(province_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE province_id IS NULL or province_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'province_name' AS column,
--       IF(province_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE province_name IS NULL or province_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'district_id' AS column,
--       IF(district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--  FROM base
--   WHERE district_id IS NULL or district_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'district_name' AS column,
--       IF(district_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE district_name IS NULL or district_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'sub_district_id' AS column,
--       IF(sub_district_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_district_id IS NULL or sub_district_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'sub_district_name' AS column,
--       IF(sub_district_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sub_district_name IS NULL or sub_district_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'zip_code' AS column,
--       IF(zip_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE zip_code IS NULL or zip_code = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'phone' AS column,
--       IF(phone IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE phone IS NULL or phone = ''

--   UNION ALL

--    SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'email' AS column,
--       IF(email IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE email IS NULL or email = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'website' AS column,
--       IF(website IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE website IS NULL or website = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'npwp_num' AS column,
--       IF(npwp_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE npwp_num IS NULL or npwp_num = ''
  
--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'siup_num' AS column,
--       IF(siup_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE siup_num IS NULL or siup_num = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'siup_expiry_date' AS column,
--       'Column can not be NULL' AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE siup_expiry_date IS NULL

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'director_ktp_num' AS column,
--       IF(director_ktp_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE director_ktp_num IS NULL or director_ktp_num = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'director_name' AS column,
--       IF(director_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE director_name IS NULL or director_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'credit_Limit' AS column,
--       IF(credit_Limit IS NULL, 'Column can not be NULL', IF(credit_Limit = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE credit_Limit IS NULL or credit_Limit <= 0

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'partnership_type' AS column,
--       IF(partnership_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE partnership_type IS NULL or partnership_type = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'npwp_file' AS column,
--       IF(company_document.npwp_file IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_document.npwp_file IS NULL or company_document.npwp_file = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'siup_file' AS column,
--       IF(company_document.siup_file IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_document.siup_file IS NULL or company_document.siup_file = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'director_ktp_file' AS column,
--       IF(company_document.director_ktp_file IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE company_document.director_ktp_file IS NULL or company_document.director_ktp_file = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'latitude' AS column,
--      'Column can not be NULL' AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE latitude IS NULL

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'longitude' AS column,
--       'Column can not be NULL' AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE longitude IS NULL

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'pks_num' AS column,
--       IF(pks_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE pks_num IS NULL or pks_num = ''

--   UNION ALL

--    SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'pks_expiry_date' AS column,
--       'Column can not be NULL'AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE pks_expiry_date IS NULL

--   UNION ALL


--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'available_credit' AS column,
--       IF(available_credit IS NULL, 'Column can not be NULL', IF(available_credit = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE available_credit IS NULL or available_credit <= 0

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'mandiri_invoice_id' AS column,
--       IF(mandiri_invoice_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE mandiri_invoice_id IS NULL or mandiri_invoice_id = ''

--   UNION ALL

--    SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'avatar' AS column,
--       IF(avatar IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE avatar IS NULL or avatar = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'form' AS column,
--       IF(form IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE form IS NULL or form = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'due_date' AS column,
--       IF(due_date IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE due_date IS NULL or due_date = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'payment_type' AS column,
--       IF(payment_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE payment_type IS NULL or payment_type = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_name' AS column,
--       IF(bank_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_name IS NULL or bank_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_transfer_code' AS column,
--       IF(bank_transfer_code IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_transfer_code IS NULL or bank_transfer_code = ''

--   UNION ALL

--    SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'fee_percentage' AS column,
--       IF(fee_percentage IS NULL, 'Column can not be NULL', IF(available_credit = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE fee_percentage IS NULL or fee_percentage <= 0

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'sales_id' AS column,
--       IF(sales_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sales_id IS NULL or sales_id = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'sales_name' AS column,
--       IF(sales_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE sales_name IS NULL or sales_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_account_name' AS column,
--       IF(bank_account_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_account_name IS NULL or bank_account_name = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'bank_account_num' AS column,
--       IF(bank_account_num IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE bank_account_num IS NULL or bank_account_num = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'tracking' AS column,
--       IF(tracking IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE tracking IS NULL or tracking = ''

--   UNION ALL

--   -- START VISIBILITY

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'visibility.feature' AS column,
--       IF(visibility.feature IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE visibility.feature IS NULL or visibility.feature = ''

--   UNION ALL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'visibility.document' AS column,
--       IF(visibility.document IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE visibility.document IS NULL or visibility.document = ''

--   -- END VISIBILITY

--   UNION ALL

--   -- START CONTROL

--   SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'control.feature' AS column,
--       IF(control.feature IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE control.feature IS NULL or control.feature = ''

--   UNION ALL

--    SELECT
--     company_id,
--     published_timestamp,

--     STRUCT(
--       'control.document' AS column,
--       IF(control.document IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM base
--   WHERE control.document IS NULL or control.document = ''

--   -- END CONTROL

-- )
-- -- END CHECK

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
  A.*,
--   B.quality_check,
FROM 
  base A
--   LEFT JOIN aggregated_check B
--   ON A.company_id = B.company_id
--   AND A.published_timestamp = B.published_timestamp
