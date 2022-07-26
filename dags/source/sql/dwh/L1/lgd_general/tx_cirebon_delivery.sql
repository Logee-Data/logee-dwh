WITH base AS (
  SELECT
    DATE(tgl_order) AS so_date_wib,
    DATE(tgl_rencana_kirim) AS planned_send_date_wib,
    DATE(tgl_terkirim) AS received_date_wib,
    replace(_cust,'NRE1','') as id_customer,
    _so as sales_order_id,
    _do as invoice_id,
    `logee-data-prod.stg_sheets.tx_cirebon_ngirim`._prod as product_id,
    `logee-data-prod.stg_sheets.tx_cirebon_ngirim`.nama_produk as product_name,
    CAST(so_qty AS INT64) AS so_quantity,
    GTV__rp_ AS so_amount,
    CAST(qty_do AS INT64) AS do_quantity,
    amount_do AS do_amount,
  FROM `logee-data-prod.stg_sheets.tx_cirebon_ngirim`
)

, with_ppn AS (
  SELECT
    *,
    ROUND(IF(so_date_wib < '2022-04-01', so_amounT * 1.1, so_amount * 1.11), 2) AS so_amount_with_ppn,
    ROUND(IF(so_date_wib < '2022-04-01', do_amounT * 1.1, do_amount * 1.11), 2) AS do_amount_with_ppn,
    (((p*l*t)/qty_karton)*so_quantity)/1000 as so_dimension_in_litres
  FROM
    base join `logee-data-prod.stg_sheets.product_dimension` on base.product_name=`logee-data-prod.stg_sheets.product_dimension`.nama_produk
)

,stg AS (
  SELECT
    so_date_wib,
    planned_send_date_wib,
    received_date_wib,
    DATE_DIFF(received_date_wib,planned_send_date_wib,DAY) as received_day_lag,
    id_customer,
    sales_order_id,
    invoice_id,
    -- dense_rank() OVER(PARTITION BY id_customer ORDER BY sales_order_id) purchase_order,
    ROW_NUMBER() OVER(PARTITION BY id_customer ORDER BY date(so_date_wib)) AS purchase_order_ordinal,
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
    -- array_agg(product_id) as product_id,
    -- array_agg(product_name) as product_name,
    -- array_agg(so_quantity)as so_quantity,
    -- array_agg(so_amount)as so_amount,
    -- array_agg(do_quantity)as do_quantity,
    -- array_agg(do_amount)as do_amount,
    -- array_agg(so_amount_with_ppn) as so_amount_with_ppn,
    -- array_agg(do_amount_with_ppn) as do_amount_with_ppn,
    sum(so_amount_with_ppn) as sum_so_amount_with_ppn,
    sum(do_amount_with_ppn) as sum_do_amount_with_ppn,
    sum(so_dimension_in_litres) as so_dimension
  FROM with_ppn group by
    so_date_wib,
    planned_send_date_wib,
    received_date_wib,
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