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

SELECT
  *
FROM
  lgd_orders_pool_status