SELECT
  *
  EXCEPT (
    quality_check
  )
FROM  
  `logee-data-prod.L2_visibility.lgd_sales`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'