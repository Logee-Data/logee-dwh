SELECT
  sub_product_id,
  sub_product_variant.variant_name AS sub_product_variant_variant_name,
  sub_product_variant.variant AS sub_product_variant_variant,
  created_at,
  modified_at,
  published_timestamp
FROM
  logee-data-prod.L2_visibility.lgd_sub_product
WHERE
  modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}',
  UNNEST(sub_product_variant) AS sub_product_variant