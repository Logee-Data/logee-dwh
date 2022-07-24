WITH base AS (
  SELECT
    DATE(PARSE_DATETIME("%d/%m/%Y %k:%M", tanggal_so)) AS so_date,
    DATE(PARSE_DATETIME("%d/%m/%Y %k:%M", tanggal_rencana_kirim)) AS planned_send_date,
    DATE(PARSE_DATETIME("%d/%m/%Y %k:%M", tanggal_terkirim)) AS received_date,
    replace(id_cust,'NRE1','') as id_customer,
    id_so as sales_order_id,
    id_do as invoice_id,
    id_produk as product_id,
    nama_produk as product_name,
    CAST(qty_so AS INT64) AS so_quantity,
    CAST(REPLACE(amount_so, ',', '.') AS FLOAT64) AS so_amount,
    CAST(qty_do AS INT64) AS do_quantity,
    CAST(REPLACE(amount_do, ',', '.') AS FLOAT64) AS do_amount,
  FROM `logee-data-prod.stg_sheets.tx_bdg_ngirim`
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
    id_customer,
    sales_order_id,
    invoice_id,
    ROW_NUMBER() OVER(PARTITION BY id_customer ORDER BY date(so_date)) AS purchase_order_ordinal,
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
    id_customer,
    sales_order_id,
    invoice_id
)

-- finalized
SELECT
  *,
  CURRENT_TIMESTAMP() AS published_timestamp
FROM
  stg
