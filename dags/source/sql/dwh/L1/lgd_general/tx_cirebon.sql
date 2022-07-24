WITH base AS (
  SELECT
    DATE(PARSE_DATETIME("%d/%m/%Y %k:%M", so_date)) AS so_date,
    DATE(PARSE_DATETIME("%d/%m/%Y %k:%M", planned_send_date)) AS planned_send_date,
    DATE(PARSE_DATETIME("%d/%m/%Y %k:%M", received_date)) AS received_date,
    customer,
    sales_order_id as sales_order_id,
    invoice_id as invoice_id,
    product_id as product_id,
    product_name as product_name,
    CAST(so_quantity AS INT64) AS so_quantity,
    CAST(REPLACE(so_amount, ',', '.') AS FLOAT64) AS so_amount,
    CAST(do_quantity AS INT64) AS do_quantity,
    CAST(REPLACE(do_amount, ',', '.') AS FLOAT64) AS do_amount,
  FROM `logee-data-prod.stg_sheets.tx_cirebon`
)

, with_ppn AS (
  SELECT
    *,
    ROUND(IF(so_date < '2022-04-01', so_amounT * 1.1, so_amount * 1.11), 2) AS so_amount_with_ppn,
    ROUND(IF(so_date < '2022-04-01', do_amounT * 1.1, do_amount * 1.11), 2) AS do_amount_with_ppn,
    (((long*width*height)/qty_ctn)*so_quantity)/1000 as so_dimension
  FROM
    base join `logee-data-prod.stg_sheets.product_dimension` on base.product_name=`logee-data-prod.stg_sheets.product_dimension`.nama_produk
)

,stg AS (
  SELECT
    so_date,
    planned_send_date,
    received_date,
    DATE_DIFF(received_date,planned_send_date,DAY) as received_day_lag,
    customer,
    sales_order_id,
    invoice_id,
    ROW_NUMBER() OVER(PARTITION BY customer ORDER BY date(so_date)) AS purchase_order_ordinal,
    ARRAY_AGG(
      STRUCT(
        product_id,
        product_name,
        so_quantity,
        so_amount,
        do_quantity,
        do_amount,
        so_amount_with_ppn,
        do_amount_with_ppn
      )
    ) AS `order`,
    sum(so_amount_with_ppn) as sum_so_amount_with_ppn,
    sum(do_amount_with_ppn) as sum_do_amount_with_ppn,
    sum(so_dimension) as so_dimension
  FROM with_ppn group by
    so_date,
    planned_send_date,
    received_date,
    customer,
    sales_order_id,
    invoice_id
)

-- finalized
SELECT
  *,
  CURRENT_TIMESTAMP() AS published_timestamp
FROM
  stg
