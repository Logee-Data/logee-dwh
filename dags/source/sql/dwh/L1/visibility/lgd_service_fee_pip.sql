SELECT  
  REPLACE(JSON_EXTRACT(data, '$.product_id'),'"','') AS product_id,
  REPLACE(JSON_EXTRACT(data, '$.product_seller_id'),'"','') AS product_seller_id,
  REPLACE(JSON_EXTRACT(data, '$.product_name'),'"','') AS product_name,
  CAST(JSON_EXTRACT(data, '$.weight') AS FLOAT64) AS weight,
  REPLACE(JSON_EXTRACT(data, '$.weight_metrics'),'"','') AS weight_metrics,
  CAST(JSON_EXTRACT(data, '$.price') AS FLOAT64)  AS price,
  CAST(JSON_EXTRACT(data, '$.konstanta') AS INT64) AS konstanta,
  CAST(JSON_EXTRACT(data, '$.revenue_sharing') AS FLOAT64) AS revenue_sharing,
  CAST(ts AS TIMESTAMP) AS published_timestamp
FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_service_fee_pip` 
WHERE 
  _date_partition IN ('{{ ds }}', '{{ next_ds }}')
  AND ts BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'