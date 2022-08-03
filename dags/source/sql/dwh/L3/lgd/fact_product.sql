SELECT
    *
    EXCEPT (
        product_variant
    )
FROM
    `logee-data-prod.L2_visibility.lgd_product`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}' 
