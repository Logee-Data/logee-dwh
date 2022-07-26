WITH base AS (
  SELECT
    *
    REPLACE(
      TO_HEX(SHA256(origin_pic_name)) AS origin_pic_name,
      TO_HEX(SHA256(origin_pic_phone)) AS origin_pic_phone,
      TO_HEX(SHA256(destination_pic_name)) AS destination_pic_name,
      TO_HEX(SHA256(destination_pic_phone)) AS destination_pic_phone,
      STRUCT (
        TO_HEX(SHA256(fulfillment.tid_num)) AS tid_num,
        TO_HEX(SHA256(fulfillment.driver_name)) AS driver_name,
        TO_HEX(SHA256(fulfillment.driver_email)) AS driver_email,
        TO_HEX(SHA256(fulfillment.driver_image)) AS driver_image,
        TO_HEX(SHA256(fulfillment.driver_phone)) AS driver_phone,
        TO_HEX(SHA256(fulfillment.confirmed_by_name)) AS confirmed_by_name,
        TO_HEX(SHA256(fulfillment.driver_sim_number)) AS driver_sim_number,
        TO_HEX(SHA256(fulfillment.confirmed_by_email)) AS confirmed_by_email,
        TO_HEX(SHA256(fulfillment.vehicle_police_num)) AS vehicle_police_num
      ) AS fulfillment,
      TO_HEX(SHA256(cargo_invoice_file)) AS cargo_invoice_file,
      TO_HEX(SHA256(truck_invoice_file)) AS truck_invoice_file,
      TO_HEX(SHA256(delivery_order_file)) AS delivery_order_file
    )
  FROM
    `logee-data-prod.L1_visibility.dma_logee_delivery_details`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

SELECT
  *
  EXCEPT (
    destination_list
  )
FROM
  base