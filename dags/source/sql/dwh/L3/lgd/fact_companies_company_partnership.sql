WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.L2_visibility.lgd_companies`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

,first_explode AS (
  SELECT
    company_id,
    STRUCT (
      company_partnership.partnership_company_id AS partnership_company_id
    ) AS company_partnership,
    modified_at
    ,
    created_at,
    published_timestamp
  FROM
    base,
    UNNEST(company_partnership) AS company_partnership
)

,lgd_companies_company_partnership AS (
  SELECT
    company_id,
    company_partnership.partnership_company_id AS partnership_company_id,
    modified_at,
    created_at,
    published_timestamp
  FROM
    first_explode
)
select
*
FROM lgd_companies_company_partnership