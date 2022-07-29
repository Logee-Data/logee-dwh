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
    order_product.sub_product_id AS order_product_sub_product_id,
    order_product.brand_id AS order_product_brand_id,
    order_product.company_id AS order_product_company_id,
    order_product.created_at AS order_product_created_at,
    order_product.created_by AS order_product_created_by,
    order_product.external_id AS order_product_external_id,
    order_product.is_bonus AS order_product_is_bonus,
    order_product.is_deleted AS order_product_is_deleted,
    order_product.is_tax AS order_product_is_tax,
    order_product.modified_at AS order_product_modified_at,
    order_product.modified_by AS order_product_modified_by,
    order_product.on_shelf AS order_product_on_shelf,
    order_product.product_id AS order_product_product_id,
    order_product.product_on_shelf AS order_product_product_on_shelf,
    order_product.sub_product_description AS order_product_sub_product_description,
    order_product.sub_product_discount_percent AS order_product_sub_product_discount_percent,
    order_product.sub_product_minimum_order AS order_product_sub_product_minimum_order,
    order_product.sub_product_name AS order_product_sub_product_name,
    order_product.sub_product_price AS order_product_sub_product_price,
    order_product.sub_product_stock AS order_product_sub_product_stock,
    order_product.sub_product_unit AS order_product_sub_product_unit,
    order_product.sub_product_weight AS order_product_sub_product_weight,
    order_product.sub_product_size AS order_product_sub_product_size,
    order_product.sub_product_stock_on_hold AS order_product_sub_product_stock_on_hold,
    order_product.sub_product_order_stock_status AS order_product_sub_product_order_stock_status,
    order_product.sub_product_minimum_order_stock_status AS order_product_sub_product_minimum_order_stock_status,
    order_product.product_name AS order_product_product_name,
    order_product.sub_product_discount_amount AS order_product_sub_product_discount_amount,
    order_product.sub_product_discount_price AS order_product_sub_product_discount_price,
    order_product.sub_product_total_discount_price AS order_product_sub_product_total_discount_price,
    order_product.sub_product_order_amount AS order_product_sub_product_order_amount,
    order_product.sub_product_total_price AS order_product_sub_product_total_price,
    order_product.sub_product_order_notes AS order_product_sub_product_order_notes,
    order_product.category_ids AS order_product_category_ids,
    order_product.sub_product_image AS order_product_sub_product_image,
    order_product.sub_product_variant AS order_product_sub_product_variant,
    modified_at,
    created_at,
    published_timestamp
  FROM
    first_explode
)

SELECT
  *
FROM
  lgd_orders_order_product