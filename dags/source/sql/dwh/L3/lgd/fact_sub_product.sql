SELECT 
    * 
    EXCEPT (
    sub_product_variant,
    booked_stock,
    quality_check
  )
FROM 
    `logee-data-prod.L2_visibility.lgd_sub_product`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'