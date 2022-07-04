WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.L1_visibility.lgd_category`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

-- BEGIN CHECK
,check AS (
  SELECT
    category_id,
    published_timestamp,
    STRUCT(
      'category_id' AS column,
      IF(category_id IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    category_id IS NULL or category_id = ''
  
  UNION ALL

  SELECT
    category_id,
    published_timestamp,
    STRUCT(
      'category_name' AS column,
      IF(category_name IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    category_name IS NULL or category_name = ''
  
  UNION ALL

  SELECT
    category_id,
    published_timestamp,
    STRUCT(
      'category_image' AS column,
      IF(category_image	 IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    category_image IS NULL or category_image = ''
  
  UNION ALL

  SELECT
    category_id,
    published_timestamp,
    STRUCT(
      'category_type' AS column,
      IF(category_type	 IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    category_type IS NULL or category_type = ''
  
  UNION ALL

  SELECT
    category_id,
    published_timestamp,
    STRUCT(
      'total_product_bind' AS column,
      IF(total_product_bind IS NULL, 'Column can not be NULL', IF(total_product_bind = 0, 'Column can not be equal to zero', "Column can not be a negative number")) AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    total_product_bind IS NULL or total_product_bind <= 0

  UNION ALL

  SELECT
    category_id,
    published_timestamp,
    STRUCT(
      'parent_id' AS column,
      IF(category_type	 IS NULL, 'Column can not be NULL', 'Column can not be an empty string') AS quality_notes
    ) AS quality_check
  FROM
    base
  WHERE
    parent_id IS NULL or parent_id = ''
)
-- END CHECK

,aggregated_check AS (
  SELECT 
    category_id,
    published_timestamp,
    ARRAY_AGG(
      quality_check
    ) AS quality_check
  FROM check
  GROUP BY 1, 2
)

SELECT
  A.*,
  B.quality_check
FROM
  base A
  LEFT JOIN aggregated_check B
  ON A.category_id = B.category_id
  AND A.published_timestamp = B.published_timestamp