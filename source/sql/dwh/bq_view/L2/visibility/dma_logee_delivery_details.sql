-- CHECK

WITH check AS (

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'delivery_detail_id' AS column,
      IF(delivery_detail_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE delivery_detail_id IS NULL or delivery_detail_id = ''

  -- available to all

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'cargo_compnay_id' AS column,
      IF(cargo_company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE cargo_company_id IS NULL or cargo_company_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'cargo_company_name' AS column,
      IF(cargo_company_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE cargo_company_name IS NULL or cargo_company_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'delivery_notes' AS column,
      IF(delivery_notes IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE delivery_notes IS NULL or delivery_notes = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'distance' AS column,
      IF(distance IS NULL, 'Column can not be NULL', IF(distance = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE distance IS NULL or distance <= 0

  -- delivery helper

  UNION ALL

   SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'escort_id' AS column,
      IF(escort_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE escort_id IS NULL or escort_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'insurance_amount' AS column,
      IF(insurance_amount IS NULL, 'Column can not be NULL', IF(insurance_amount = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE insurance_amount IS NULL or insurance_amount <= 0

  UNION ALL

 SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_category_id' AS column,
      IF(item_category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_category_id IS NULL or item_category_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_category_name' AS column,
      IF(item_category_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_category_name IS NULL or item_category_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_height' AS column,
      IF(item_height IS NULL, 'Column can not be NULL', IF(item_height = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_height IS NULL or item_height <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_length' AS column,
      IF(item_length IS NULL, 'Column can not be NULL', IF(item_length = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_length IS NULL or item_length <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_length' AS column,
      IF(item_width IS NULL, 'Column can not be NULL', IF(item_width = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_width IS NULL or item_width <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_name' AS column,
      IF(item_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_name IS NULL or item_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_package' AS column,
      IF(item_package IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_package IS NULL or item_package = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'item_value' AS column,
      IF(item_value IS NULL, 'Column can not be NULL', IF(item_value = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE item_value IS NULL or item_value <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'order_number' AS column,
      IF(order_number IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE order_number IS NULL or order_number = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'payment_type' AS column,
      IF(payment_type IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE payment_type IS NULL or payment_type = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'payment_type_id' AS column,
      IF(payment_type_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE payment_type_id IS NULL or payment_type_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'payment_type_image' AS column,
      IF(payment_type_image IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE payment_type_image IS NULL or payment_type_image = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'payment_type_name' AS column,
      IF(payment_type_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE payment_type_name IS NULL or payment_type_name = ''

  --pick up time

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'tax_amount' AS column,
      IF(tax_amount IS NULL, 'Column can not be NULL', IF(tax_amount = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE tax_amount IS NULL or tax_amount <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'total_amount' AS column,
      IF(total_amount IS NULL, 'Column can not be NULL', IF(total_amount = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE total_amount IS NULL or total_amount <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'trip_fee_amount' AS column,
      IF(trip_fee_amount IS NULL, 'Column can not be NULL', IF(trip_fee_amount = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE trip_fee_amount IS NULL or trip_fee_amount <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'truck_company_id' AS column,
      IF(truck_company_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE truck_company_id IS NULL or truck_company_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'truck_company_name' AS column,
      IF(truck_company_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE truck_company_name IS NULL or truck_company_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'vehicle_group_id' AS column,
      IF(vehicle_group_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE vehicle_group_id IS NULL or vehicle_group_id = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'vehicle_group_name' AS column,
      IF(vehicle_group_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE vehicle_group_name IS NULL or vehicle_group_name = ''

  -- is paid
  -- send company patner pert

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_address' AS column,
      IF(origin_address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_address IS NULL or origin_address = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_city' AS column,
      IF(origin_city IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_city IS NULL or origin_city = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_location' AS column,
      IF(origin_location IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_location IS NULL or origin_location = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_latitude' AS column,
      IF(origin_latitude IS NULL, 'Column can not be NULL', IF(origin_latitude = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_latitude IS NULL or origin_latitude <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_longitude' AS column,
      IF(origin_longitude IS NULL, 'Column can not be NULL', IF(origin_longitude = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_longitude IS NULL or origin_longitude <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_pic_name' AS column,
      IF(origin_pic_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_pic_name IS NULL or origin_pic_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'origin_pic_phone' AS column,
      IF(origin_pic_phone IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE origin_pic_phone IS NULL or origin_pic_phone = ''

  -- destination count

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_address' AS column,
      IF(destination_address IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_address IS NULL or destination_address = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_city' AS column,
      IF(destination_city IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_city IS NULL or destination_city = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_location' AS column,
      IF(destination_location IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_location IS NULL or destination_location = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_latitude' AS column,
      IF(destination_latitude IS NULL, 'Column can not be NULL', IF(destination_latitude = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_latitude IS NULL or destination_latitude <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_longitude' AS column,
      IF(destination_longitude IS NULL, 'Column can not be NULL', IF(destination_longitude = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_longitude IS NULL or destination_longitude <= 0

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_pic_name' AS column,
      IF(destination_pic_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_pic_name IS NULL or destination_pic_name = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'destination_pic_phone' AS column,
      IF(destination_pic_phone IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE destination_pic_phone IS NULL or destination_pic_phone = ''

-- fulfillment
-- destinationlist
-- deliverydetailstatus

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'cargo_invoice_file' AS column,
      IF(cargo_invoice_file IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE cargo_invoice_file IS NULL or cargo_invoice_file = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'truck_invoice_file' AS column,
      IF(truck_invoice_file IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE truck_invoice_file IS NULL or truck_invoice_file = ''

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'delivery_detail_ids' AS column,
      IF(delivery_detail_ids IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE delivery_detail_ids IS NULL or delivery_detail_ids = ''

-- is active
-- is deleted
-- created by
-- created at
-- modified by
-- modified at

  UNION ALL

  SELECT
    original_data,
    published_timestamp,
    STRUCT(
      'delivery_order_file' AS column,
      IF(delivery_order_file IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM `logee-data-dev.L1_visibility.dma_logee_deliverydetails`
  WHERE delivery_order_file IS NULL or delivery_order_file = ''

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
  `logee-data-dev.L1_visibility.dma_logee_deliverydetails` A
  LEFT JOIN aggregated_check B
  ON A.original_data = B.original_data
  AND A.published_timestamp = B.published_timestamp