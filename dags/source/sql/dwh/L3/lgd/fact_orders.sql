SELECT
  *
  EXCEPT (
    quality_check,
    pool_status,
    order_product
  )
  REPLACE (
    STRUCT(
      payment.payment_id,
      payment.payment_type,
      payment.payment_fee,
      payment.company_id,
      payment.payment_metadata.payment_method_fee,
      payment.payment_metadata.payment_gateway_fee
    ) AS payment
  )
FROM
  `logee-data-prod.L2_visibility.lgd_orders`
WHERE
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'