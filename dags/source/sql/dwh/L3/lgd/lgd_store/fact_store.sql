select
*
EXCEPT (
    company_partnership,
    quality_check
  )
FROM `logee-data-prod.L2_visibility.lgd_store`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'