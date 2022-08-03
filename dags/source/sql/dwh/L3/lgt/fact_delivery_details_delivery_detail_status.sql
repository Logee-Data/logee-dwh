WITH base AS (
  SELECT
    *
  FROM  
    `logee-data-prod.L2_visibility.dma_logee_delivery_details`
  WHERE
      modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

,first_explode AS (
  SELECT
    delivery_detail_id,
    delivery_detail_status.code AS delivery_detail_status_code,
    delivery_detail_status.date AS delivery_detail_status_date,
    delivery_detail_status.name AS delivery_detail_status_name,
    created_at,
    modified_at,
    published_timestamp
  FROM
    base,
    UNNEST(delivery_detail_status) AS delivery_detail_status
)

,pre_final AS (
  SELECT
    *,
    RANK() OVER(partition by delivery_detail_id ORDER BY modified_at) AS row_rank,
    RANK() OVER(partition by delivery_detail_id, modified_at ORDER BY delivery_detail_status_date DESC) AS pool_rank,
  FROM
    first_explode
  ORDER BY 1, 5, 4
)

,final AS (
  SELECT
    *
    EXCEPT(
      row_rank, pool_rank,
      delivery_detail_status_name,
      delivery_detail_status_date
    ),
    delivery_detail_status_name AS name,
    delivery_detail_status_date AS name_modified_at,
    ROW_NUMBER() OVER(PARTITION BY delivery_detail_id, delivery_detail_status_name ORDER BY delivery_detail_status_date) rn
  FROM
    pre_final
  WHERE pool_rank = 1
  ORDER by row_rank, pool_rank, delivery_detail_status_date
)

SELECT
  *
  EXCEPT(
    rn
  )
FROM
  final
WHERE
  rn = 1
  AND CONCAT(delivery_detail_id, '_', CAST(name_modified_at AS STRING), '_', name) NOT IN (
    SELECT
      DISTINCT CONCAT(delivery_detail_id, '_', CAST(name_modified_at AS STRING), '_', name)
    FROM
      `logee-data-prod.L3_lgd.fact_delivery_details_delivery_detail_status`
  )