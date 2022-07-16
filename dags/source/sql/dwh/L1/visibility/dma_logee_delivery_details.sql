WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.logee_datalake_raw_production.visibility_dma_logee_deliverydetails`
  WHERE
    _date_partition IN ('{{ ds }}', '{{ next_ds }}')
    AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

--Begin deliveryDetailStatus
,delivery_detail_status AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT (
        IF(JSON_EXTRACT_SCALAR(delivery_detail_status, '$.code') = "", NULL, JSON_EXTRACT_SCALAR(delivery_detail_status, '$.code')) AS code,
        CAST(IF(JSON_EXTRACT_SCALAR(delivery_detail_status, '$.date') = "", NULL, JSON_EXTRACT_SCALAR(delivery_detail_status, '$.date')) AS TIMESTAMP) AS date,
        IF(JSON_EXTRACT_SCALAR(delivery_detail_status, '$.name') = "", NULL, JSON_EXTRACT_SCALAR(delivery_detail_status, '$.name')) AS name
      )
    ) AS delivery_detail_status
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.deliveryDetailStatus')) AS delivery_detail_status
  GROUP BY 1,2
)
--end

--Begin destinationList
,destination_list AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT (
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.itemName') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.itemName')) AS item_name,
        CAST(IF(JSON_EXTRACT_SCALAR(destination_list, '$.itemValue') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.itemValue')) AS FLOAT64) AS item_value,
        CAST(IF(JSON_EXTRACT_SCALAR(destination_list, '$.itemWeight') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.itemWeight')) AS FLOAT64) AS item_weight,
        CAST(IF(JSON_EXTRACT_SCALAR(destination_list, '$.isOtpVerified') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.isOtpVerified')) AS BOOL) AS is_otp_verified,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.itemCategoryId') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.itemCategoryId')) AS item_category_id,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.itemWeightUnit') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.itemWeightUnit')) AS item_weight_unit,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationCity') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationCity')) AS destination_city,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationNotes') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationNotes')) AS destination_notes,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationPoint') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationPoint')) AS destination_point,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.itemCategoryName') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.itemCategoryName')) AS item_category_name,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationAddress') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationAddress')) AS destination_address,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationPicName') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationPicName')) AS destination_pic_name,
        CAST(IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationLatitude') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationLatitude')) AS FLOAT64) AS destination_latitude,
        CAST(IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationLongitude') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationLongitude')) AS FLOAT64) AS destination_longitude,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationLocation') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationLocation')) AS destination_location,
        IF(JSON_EXTRACT_SCALAR(destination_list, '$.destinationPicPhone') = "", NULL, JSON_EXTRACT_SCALAR(destination_list, '$.destinationPicPhone')) AS destination_pic_phone
      )
    ) AS destination_list
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.destinationList')) AS destination_list
  GROUP BY 1,2
)


SELECT
  REPLACE(JSON_EXTRACT(A.data, '$.deliveryDetailId'), '"', '') AS delivery_detail_id,
  CAST(IF(JSON_EXTRACT_SCALAR(A.data, '$.availableToAll') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.availableToAll')) AS BOOLEAN) AS available_to_all,
  IF(JSON_EXTRACT_SCALAR(A.data, '$.cargoCompanyId') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.cargoCompanyId')) AS cargo_company_id,
  IF(JSON_EXTRACT_SCALAR(A.data, '$.cargoCompanyName') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.cargoCompanyName')) AS cargo_company_name,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.deliveryNotes'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.deliveryNotes'), '"', '')) AS delivery_notes,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.distance') AS FLOAT64) AS distance,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.driverHelper') AS INT64) AS driver_helper,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.escortId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.escortId'), '"', '')) AS escort_id,
  CAST(IF(JSON_EXTRACT_SCALAR(A.data, '$.insuranceAmount') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.insuranceAmount')) AS FLOAT64) AS insurance_amount,
  REPLACE(JSON_EXTRACT(A.data, '$.itemCategoryId'), '"', '') AS item_category_id,
  REPLACE(JSON_EXTRACT(A.data, '$.itemCategoryName'), '"', '') AS item_category_name,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.itemHeight') AS FLOAT64) AS item_height,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.itemLength') AS FLOAT64) AS item_length,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.itemWeight') AS FLOAT64) AS item_weight,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.itemWidth') AS FLOAT64) AS item_width,
  REPLACE(JSON_EXTRACT(A.data, '$.itemName'), '"', '') AS item_name,
  REPLACE(JSON_EXTRACT(A.data, '$.itemPackage'), '"', '') AS item_package,
  CAST(IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.itemValue'), ' ', '') = '', NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.itemValue'), ' ', '')) AS FLOAT64) AS item_value,
  REPLACE(JSON_EXTRACT(A.data, '$.orderNumber'), '"', '') AS order_number,
  REPLACE(JSON_EXTRACT(A.data, '$.paymentType'), '"', '') AS payment_type,
  REPLACE(JSON_EXTRACT(A.data, '$.paymentTypeId'), '"', '') AS payment_type_id,
  REPLACE(JSON_EXTRACT(A.data, '$.paymentTypeImage'), '"', '') AS payment_type_image,
  REPLACE(JSON_EXTRACT(A.data, '$.paymentTypeName'), '"', '') AS payment_type_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.pickUpTime'), '"', '') AS TIMESTAMP) AS pick_up_time,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.pricingId'), '"', '') = "", NULL, JSON_EXTRACT(A.data, '$.pricingId')) AS pricing_id,
  CAST(IF(JSON_EXTRACT_SCALAR(A.data, '$.taxAmount') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.taxAmount')) AS FLOAT64) AS tax_amount,
  CAST(IF(JSON_EXTRACT_SCALAR(A.data, '$.totalAmount') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.totalAmount')) AS FLOAT64) AS total_amount,
  CAST(IF(JSON_EXTRACT_SCALAR(A.data, '$.tripFeeAmount') = "", NULL, JSON_EXTRACT_SCALAR(A.data, '$.tripFeeAmount')) AS FLOAT64) AS trip_fee_amount,
  REPLACE(JSON_EXTRACT(A.data, '$.truckCompanyId'), '"', '') AS truck_company_id,
  REPLACE(JSON_EXTRACT(A.data, '$.truckCompanyName'), '"', '') AS truck_company_name,
  REPLACE(JSON_EXTRACT(A.data, '$.vehicleGroupId'), '"', '') AS vehicle_group_id,
  REPLACE(JSON_EXTRACT(A.data, '$.vehicleGroupName'), '"', '') AS vehicle_group_name,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isPaid'), '"', '') AS BOOLEAN) AS is_paid,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.sendCompanyPartnerFirt'), '"', '') AS BOOLEAN) AS send_company_partner_firt,
  REPLACE(JSON_EXTRACT(A.data, '$.originAddress'), '"', '') AS origin_address,
  REPLACE(JSON_EXTRACT(A.data, '$.originCity'), '"', '') AS origin_city,
  REPLACE(JSON_EXTRACT(A.data, '$.originLocation'), '"', '') AS origin_location,
  REPLACE(JSON_EXTRACT(A.data, '$.originLatitude'), '"', '') AS origin_latitude,
  REPLACE(JSON_EXTRACT(A.data, '$.originLongitude'), '"', '') AS origin_longitude,
  REPLACE(JSON_EXTRACT(A.data, '$.originPicName'), '"', '') AS origin_pic_name,
  REPLACE(JSON_EXTRACT(A.data, '$.originPicPhone'), '"', '') AS origin_pic_phone,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.destinationCount'), '"', '') AS INT64) AS destination_count,
  REPLACE(JSON_EXTRACT(A.data, '$.destinationAddress'), '"', '') AS destination_address,
  REPLACE(JSON_EXTRACT(A.data, '$.destinationCity'), '"', '') AS destination_city,
  REPLACE(JSON_EXTRACT(A.data, '$.destinationLocation'), '"', '') AS destination_location,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.destinationLatitude'), '"', '') AS FLOAT64) AS destination_latitude,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.destinationLongitude'), '"', '') AS FLOAT64) AS destination_longitude,
  REPLACE(JSON_EXTRACT(A.data, '$.destinationPicName'), '"', '') AS destination_pic_name,
  REPLACE(JSON_EXTRACT(A.data, '$.destinationPicPhone'), '"', '') AS destination_pic_phone,
  STRUCT(
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.tidNum'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.tidNum'), '"', '')) AS tid_num,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverId'), '"', '')) AS driver_id,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleId'), '"', '')) AS vehicle_id,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverName'), '"', '')) AS driver_name,
    CAST(IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedAt'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedAt'), '"', '')) AS TIMESTAMP) AS confirmed_at,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedBy'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedBy'), '"', '')) AS confirmed_by,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverEmail'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverEmail'), '"', '')) AS driver_email,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverImage'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverImage'), '"', '')) AS driver_image,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverPhone'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverPhone'), '"', '')) AS driver_phone,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleImage'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleImage'), '"', '')) AS vehicle_image,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverSimType'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverSimType'), '"', '')) AS driver_sim_type,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedByName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedByName'), '"', '')) AS confirmed_by_name,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverSimNumber'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.driverSimNumber'), '"', '')) AS driver_sim_number,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedByEmail'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.confirmedByEmail'), '"', '')) AS confirmed_by_email,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleBrandName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleBrandName'), '"', '')) AS vehicle_brand_name,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehiclePoliceNum'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehiclePoliceNum'), '"', '')) AS vehicle_police_num,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleManufactureYear'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.fulfillment.vehicleManufactureYear'), '"', '')) AS vehicle_manufacture_year
  ) AS fulfillment,
  B.destination_list,
  C.delivery_detail_status,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.cargoInvoiceFile'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.cargoInvoiceFile'), '"', '')) AS cargo_invoice_file,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.truckInvoiceFile'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.truckInvoiceFile'), '"', '')) AS truck_invoice_file,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.deliveryDetailIds'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.deliveryDetailIds'), '"', '')) AS delivery_detail_ids,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isActive'), '"', '') AS BOOLEAN) AS is_active,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isDeleted'), '"', '') AS BOOLEAN) AS is_deleted,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '')) AS created_by,
  CAST(IF(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '')) AS TIMESTAMP) AS created_at,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '')) AS modified_by,
  CAST(IF(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') = '', NULL, REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '')) AS TIMESTAMP) AS modified_at,
  IF(REPLACE(JSON_EXTRACT(A.data, '$.deliveryOrderFile'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.deliveryOrderFile'), '"', '')) AS delivery_order_file,
  A.ts AS published_timestamp
FROM
  base A

  LEFT JOIN destination_list B
  ON A.data = B.data
  AND A.ts = B.published_timestamp

  LEFT JOIN delivery_detail_status C
  ON A.data = C.data
  AND A.ts = C.published_timestamp

