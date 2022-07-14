WITH 

base AS(
  SELECT * REPLACE(
    TO_HEX(SHA256(police_number)) AS police_number,
    TO_HEX(SHA256(stnk_number)) AS stnk_number,
    TO_HEX(SHA256(kir_number)) AS kir_number,
    TO_HEX(SHA256(stnk_owner_name)) AS stnk_owner_name,
    TO_HEX(SHA256(tid_number)) AS tid_number,
    TO_HEX(SHA256(image_stnk)) AS image_stnk
  )
  FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
  WHERE modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- ,check AS (

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'vehicle_id' AS column,
--       IF(vehicle_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE vehicle_id IS NULL or vehicle_id = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'vehicle_group_id' AS column,
--       IF(vehicle_group_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE vehicle_group_id IS NULL or vehicle_group_id = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'vehicle_group_name' AS column,
--       IF(vehicle_group_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE vehicle_group_name IS NULL or vehicle_group_name = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'brand_id' AS column,
--       IF(brand_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE brand_id IS NULL or brand_id = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'brand_name' AS column,
--       IF(brand_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE brand_name IS NULL or brand_name = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'police_number' AS column,
--       IF(police_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE police_number IS NULL or police_number = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'manufacture_year' AS column,
--       IF(manufacture_year IS NULL, 'Column can not be NULL', 
--         IF(manufacture_year = 0, 'Column can not be equal to zero', "Column can not be a negative number")
--       ) AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE manufacture_year IS NULL or manufacture_year <= 0

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'ownership_status' AS column,
--       IF(ownership_status IS NULL, 'Column can not be NULL', 
--         IF(ownership_status = 0, 'Column can not be equal to zero', "Column can not be a negative number")
--       ) AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE ownership_status IS NULL or ownership_status <= 0

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'company_id' AS column,
--       IF(company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE company_id IS NULL or company_id = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'company_name' AS column,
--       IF(company_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE company_name IS NULL or company_name = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'kir_expiry_date' AS column,
--       IF(kir_expiry_date IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE kir_expiry_date IS NULL

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'kir_number' AS column,
--       IF(kir_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE kir_number IS NULL or kir_number = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'stnk_address' AS column,
--       IF(stnk_address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE stnk_address IS NULL or stnk_address = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'stnk_expiry_date' AS column,
--       IF(stnk_expiry_date IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE stnk_expiry_date IS NULL

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'stnk_number' AS column,
--       IF(stnk_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE stnk_number IS NULL or stnk_number = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'stnk_owner_name' AS column,
--       IF(stnk_owner_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE stnk_owner_name IS NULL or stnk_owner_name = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'tid_number' AS column,
--       IF(tid_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE tid_number IS NULL or tid_number = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'tnkb_colour' AS column,
--       IF(tnkb_colour IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE tnkb_colour IS NULL or tnkb_colour = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'image_stnk' AS column,
--       IF(image_stnk IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE image_stnk IS NULL or image_stnk = ''

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'wheels' AS column,
--       IF(wheels IS NULL, 'Column can not be NULL', 
--         IF(wheels = 0, 'Column can not be equal to zero', "Column can not be a negative number")
--       ) AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE wheels IS NULL or wheels <= 0

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'capacity_weight' AS column,
--       IF(capacity_weight IS NULL, 'Column can not be NULL', 
--         IF(capacity_weight = 0, 'Column can not be equal to zero', "Column can not be a negative number")
--       ) AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE capacity_weight IS NULL or capacity_weight <= 0

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'vehicle_status' AS column,
--       IF(vehicle_status IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE vehicle_status IS NULL

--   UNION ALL

--   SELECT
--     vehicle_id,
--     published_timestamp,

--     STRUCT(
--       'item_categories' AS column,
--       IF(item_categories IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
--     ) AS quality_check
--   FROM `logee-data-prod.L1_visibility.dma_logee_vehicles`
--   WHERE item_categories IS NULL or item_categories = ''
-- )

-- ,aggregated_check AS (
--   SELECT
--     vehicle_id,
--     published_timestamp,
--     ARRAY_AGG(
--       quality_check
--     ) AS quality_check
--   FROM check
--   GROUP BY 1, 2
-- )

SELECT 
    A.*
    -- B.quality_check
FROM base A
-- LEFT JOIN aggregated_check B
-- ON A.vehicle_id = B.vehicle_id
-- AND A.published_timestamp = B.published_timestamp