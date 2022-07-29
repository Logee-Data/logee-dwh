SELECT
  *
  EXCEPT (
    quality_check,
    pool_status,
    order_product
  )
FROM
  `logee-data-prod.L2_visibility.lgd_orders`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'