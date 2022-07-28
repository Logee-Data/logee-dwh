SELECT
  product_id,
  product_variant.variant_name AS product_variant_variant_name,
  product_variant.variant_list AS product_variant_variant_list,
  created_at,
  modified_at,
  published_timestamp
FROM
  `logee-data-prod.L2_visibility.lgd_product`
WHERE
  modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}',
  UNNEST(product_variant) AS product_variant