SELECT
    *
FROM `logee-data-prod.L2_visibility.lgd_companies`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'