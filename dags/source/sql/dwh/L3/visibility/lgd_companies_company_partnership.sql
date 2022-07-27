select
company_id,
company_partnership.partnership_company_id AS partnership_company_id,
created_at,
modified_at,
published_timestamp
FROM `logee-data-prod.L2_visibility.lgd_companies`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}',
UNNEST(company_partnership) AS company_partnership