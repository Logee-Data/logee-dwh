WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.L2_visibility.lgd_orders`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)



,first_explode AS (
  SELECT
    order_id,
    STRUCT (
      pool_status.date_time AS date_time,
      pool_status.status AS status,
      pool_status.is_change AS is_change
    ) AS pool_status,
    modified_at,
    created_at,
    published_timestamp
  FROM
    base,
    UNNEST(pool_status) AS pool_status
)

,lgd_orders_pool_status AS (
  SELECT
    order_id,
    pool_status.date_time AS pool_status_date_time,
    pool_status.status AS pool_status_status,
    pool_status.is_change AS pool_status_is_change,
    modified_at,
    created_at,
    published_timestamp
  FROM
    first_explode
)

,pre_final AS (
  SELECT
    *,
    RANK() OVER(partition by order_id ORDER BY modified_at) AS row_rank,
    RANK() OVER(partition by order_id, modified_at ORDER BY pool_status_date_time DESC) AS pool_rank,
  FROM
    lgd_orders_pool_status
  ORDER BY 1, 5, 4
)

,final AS (
  SELECT
    * EXCEPT(
      row_rank, pool_rank,
      pool_status_status, pool_status_date_time
    ),
    pool_status_status AS status,
    pool_status_date_time AS status_modified_at,
    ROW_NUMBER() OVER(PARTITION BY order_id, pool_status_status ORDER BY pool_status_date_time) rn
  FROM
    pre_final
  WHERE pool_rank = 1
  ORDER by row_rank, pool_rank, pool_status_date_time
)

SELECT
  * EXCEPT(rn)
FROM
  final
WHERE rn = 1
ORDER BY
  order_id, modified_at