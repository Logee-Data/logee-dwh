WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.L2_visibility.lgd_orders`
  WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'
)

,first_explode AS (
  SELECT
    order_id,
    STRUCT (
      order_product.sub_product_id AS sub_product_id,
      order_product.brand_id AS brand_id,
      order_product.company_id AS company_id,
      order_product.created_at AS created_at,
      order_product.created_by AS created_by,
      order_product.external_id AS external_id,
      order_product.is_bonus AS is_bonus,
      order_product.is_deleted AS is_deleted,
      order_product.is_tax AS is_tax,
      order_product.modified_at AS modified_at,
      order_product.modified_by AS modified_by,
      order_product.on_shelf AS on_shelf,
      order_product.product_id AS product_id,
      order_product.product_on_shelf AS product_on_shelf,
      order_product.sub_product_description AS sub_product_description,
      order_product.sub_product_discount_percent AS sub_product_discount_percent,
      order_product.sub_product_minimum_order AS sub_product_minimum_order,
      order_product.sub_product_name AS sub_product_name,
      order_product.sub_product_price AS sub_product_price,
      order_product.sub_product_stock AS sub_product_stock,
      order_product.sub_product_unit AS sub_product_unit,
      order_product.sub_product_weight AS sub_product_weight,
      order_product.sub_product_size AS sub_product_size,
      order_product.sub_product_stock_on_hold AS sub_product_stock_on_hold,
      order_product.sub_product_order_stock_status AS sub_product_order_stock_status,
      order_product.sub_product_minimum_order_stock_status AS sub_product_minimum_order_stock_status,
      order_product.product_name AS product_name,
      order_product.sub_product_discount_amount AS sub_product_discount_amount,
      order_product.sub_product_discount_price AS sub_product_discount_price,
      order_product.sub_product_total_discount_price AS sub_product_total_discount_price,
      order_product.sub_product_order_amount AS sub_product_order_amount,
      order_product.sub_product_total_price AS sub_product_total_price,
      order_product.sub_product_order_notes AS sub_product_order_notes,
      order_product.category_ids AS category_ids,
      order_product.sub_product_image AS sub_product_image,
      order_product.sub_product_variant AS sub_product_variant
    ) AS order_product,
    base.modified_at,
    base.created_at,
    published_timestamp
  FROM
    base,
    UNNEST(order_product) AS order_product
)

,lgd_orders_order_product AS (
  SELECT
    order_id,
    modified_at AS order_id_modified_at,
    created_at AS order_id_created_at,
    published_timestamp,

    ---
    order_product.sub_product_id AS sub_product_id,
    order_product.brand_id AS brand_id,
    order_product.company_id AS product_company_id,
    order_product.created_at AS created_at,
    order_product.created_by AS created_by,
    order_product.external_id AS external_id,
    order_product.is_bonus AS is_bonus,
    order_product.is_deleted AS is_deleted,
    order_product.is_tax AS is_tax,
    order_product.modified_at AS modified_at,
    order_product.modified_by AS modified_by,
    order_product.on_shelf AS on_shelf,
    order_product.product_id AS product_id,
    order_product.product_on_shelf AS product_on_shelf,
    order_product.sub_product_description AS sub_product_description,
    order_product.sub_product_discount_percent AS sub_product_discount_percent,
    order_product.sub_product_minimum_order AS sub_product_minimum_order,
    order_product.sub_product_name AS sub_product_name,
    order_product.sub_product_price AS sub_product_price,
    order_product.sub_product_stock AS sub_product_stock,
    order_product.sub_product_unit AS sub_product_unit,
    order_product.sub_product_weight AS sub_product_weight,
    order_product.sub_product_size AS sub_product_size,
    order_product.sub_product_stock_on_hold AS sub_product_stock_on_hold,
    order_product.sub_product_order_stock_status AS sub_product_order_stock_status,
    order_product.sub_product_minimum_order_stock_status AS sub_product_minimum_order_stock_status,
    order_product.product_name AS product_name,
    order_product.sub_product_discount_amount AS sub_product_discount_amount,
    order_product.sub_product_discount_price AS sub_product_discount_price,
    order_product.sub_product_total_discount_price AS sub_product_total_discount_price,
    order_product.sub_product_order_amount AS sub_product_order_amount,
    order_product.sub_product_total_price AS sub_product_total_price,
    order_product.sub_product_order_notes AS sub_product_order_notes,
    order_product.category_ids AS category_ids,
    order_product.sub_product_image AS sub_product_image,
    STRUCT(
      IF(
        ARRAY_LENGTH(order_product.sub_product_variant) != 0,
        JSON_EXTRACT_SCALAR(order_product.sub_product_variant[OFFSET(0)], "$.variantName"),
        NULL
      ) AS variant_name,
      IF(
        ARRAY_LENGTH(order_product.sub_product_variant) != 0,
        JSON_EXTRACT_SCALAR(order_product.sub_product_variant[OFFSET(0)], "$.variant"),
        NULL
      ) AS variant
    ) AS sub_product_variant,
    LAG(order_product.modified_at) OVER(PARTITION BY order_id, order_product.sub_product_id ORDER BY modified_at) AS previous_modified_at
  FROM
    first_explode
)


SELECT
  order_id, sub_product_id, order_id_modified_at, modified_at,
  * EXCEPT(order_id, sub_product_id, order_id_modified_at, modified_at, previous_modified_at)
FROM lgd_orders_order_product
WHERE
  modified_at != previous_modified_at
ORDER BY 1, 2, 3, 4
