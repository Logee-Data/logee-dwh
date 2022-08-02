SELECT
  sub_product_id,
  booked_stock.order_id AS booked_stock_order_id,
  booked_stock.stock AS booked_stock_stock,
  created_at,
  modified_at,
  published_timestamp
FROM
  logee-data-prod.L2_visibility.lgd_sub_product
WHERE
  modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}',
  UNNEST(booked_stock) AS booked_stock