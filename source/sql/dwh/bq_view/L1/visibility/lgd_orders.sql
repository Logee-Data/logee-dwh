WITH 

base AS (
  SELECT * FROM `logee-data-prod.logee_datalake_raw_production.visibility_lgd_orders`
  WHERE _date_partition >= '2022-01-01'
)

-- BEGIN STORE.STORE_IMAGES

,pre_store_images AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(store_images, '$.image') AS image,
      CAST(JSON_EXTRACT_SCALAR(store_images, '$.mainImage') AS BOOL) AS main_image
    ) AS store_images
  FROM base,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.store'), '$.storeImages')) AS store_images
)

,store_images AS (
  select
    data,
    published_timestamp,
    ARRAY_AGG(store_images) AS store_images
  from pre_store_images
  GROUP BY 1, 2
)

-- END STORE.STORE_IMAGES

-- BEGIN STORE.APPS

,store_apps AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      REPLACE(apps, '"', '')
    ) AS apps
  FROM base,
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.store.apps')) AS apps
  GROUP BY 1,2
)

-- END STORE.APPS

-- BEGIN STORE.LIST_ADDRESS

,pre_list_address AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(list_address, '$.storeName') AS store_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(list_address, '$.recipientName'), '"', '')) AS recipient_name,
      CAST(JSON_EXTRACT_SCALAR(list_address, '$.phoneNumber') AS INT64) AS phone_number,
      JSON_EXTRACT_SCALAR(list_address, '$.province') AS province,
      JSON_EXTRACT_SCALAR(list_address, '$.provinceId') AS province_id,
      JSON_EXTRACT_SCALAR(list_address, '$.city') AS city,
      JSON_EXTRACT_SCALAR(list_address, '$.cityId') AS city_id,
      JSON_EXTRACT_SCALAR(list_address, '$.district') AS district,
      JSON_EXTRACT_SCALAR(list_address, '$.districtId') AS district_id,
      JSON_EXTRACT_SCALAR(list_address, '$.subDistrict') AS sub_district,
      JSON_EXTRACT_SCALAR(list_address, '$.subDistrictId') AS sub_district_id,
      JSON_EXTRACT_SCALAR(list_address, '$.zipCode') AS zip_code,
      JSON_EXTRACT_SCALAR(list_address, '$.zipCodeId') AS zipcode_id,
      JSON_EXTRACT_SCALAR(list_address, '$.address') AS address,
      JSON_EXTRACT_SCALAR(list_address, '$.mainAddress') AS main_address,
      IF(JSON_EXTRACT_SCALAR(list_address, '$.externalId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.externalId')) AS external_id,
      JSON_EXTRACT_SCALAR(list_address, '$.lat') AS lat,
      JSON_EXTRACT_SCALAR(list_address, '$.long') AS long,
      CAST(JSON_EXTRACT_SCALAR(list_address, '$.isFulfillmentProcess') AS BOOL) AS is_fulfillment_process,
      JSON_EXTRACT_SCALAR(list_address, '$.addressId') AS address_id,
      IF(JSON_EXTRACT_SCALAR(list_address, '$.addressMark') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.addressMark')) AS address_mark
    ) AS list_address
  FROM base,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.store'), '$.listAddress')) AS list_address
)

,list_address AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG(list_address) AS list_address
  FROM pre_list_address
  GROUP BY 1, 2
)

-- END STORE.LIST_ADDRESS

-- BEGIN STORE.MAIN_ADDRESS

,main_address AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.storeName') AS store_name,
      IF(REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.recipientName'), '"', ''))  AS recipient_name,
      CAST(JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.phoneNumber') AS INT64) AS phone_number,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.province') AS province,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.provinceId	') AS province_id,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.city') AS city,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.cityId') AS city_id,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.district') AS district,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.districtId') AS district_id,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.subDistrict') AS sub_district,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.subDistrictId') AS sub_district_id,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.zipCode') AS zip_code,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.zipCodeId') AS zipcode_id,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.address') AS address,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.mainAddress') AS main_address,
      IF(JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.externalId') = "", NULL, JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.externalId')) AS external_id,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.lat') AS lat,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.long') AS long,
      CAST(JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.isFulfillmentProcess') AS BOOL) AS is_fulfillment_process,
      JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.addressId') AS address_id,
      IF(JSON_EXTRACT_SCALAR(data, '$.store.mainAddress.addressMark') = "", NULL, JSON_EXTRACT(data, '$.store.mainAddress.addressMark')) AS address_mark
    ) AS main_address
  FROM base
)
-- END STORE.MAIN_ADDRESS

-- BEGIN STORE.STORE_OWNER

,store_owner AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT (
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerName'), '"', '') AS owner_name,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerPhoneNumber'), '"', '') AS INT64) AS owner_phone_number,
      STRUCT (
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.province'), '"', '') AS province,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.provinceId'), '"', '') AS province_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.city'), '"', '') AS city,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.cityId'), '"', '') AS city_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.district'), '"', '') AS district,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.districtId'), '"', '') AS district_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.subDistrict'), '"', '') AS sub_district,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.subDistrictId'), '"', '') AS sub_district_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.zipCode'), '"', '') AS zip_code,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.zipCodeId'), '"', '') AS zip_code_id,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.address'), '"', '') AS address,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.lat'), '"', '') AS lat,
        REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.storeOwner.ownerAddress.long'), '"', '') AS long
      ) AS owner_address
    ) AS store_owner
  FROM
    base
)

-- END STORE.STORE_OWNER

-- BEGIN STORE.METADATA

,store_metadata AS (
  SELECT
    data,
    ts AS published_timestamp,
    STRUCT (
      REPLACE(JSON_EXTRACT_SCALAR(data, '$.store.metadata.firstOrderId'), '"', '') AS first_order_id
    ) AS metadata
  FROM base
)

-- END STORE.METADATA

-- BEGIN STORE

,store AS (
  SELECT
    A.data,
    ts AS published_timestamp,
    STRUCT(
      JSON_EXTRACT_SCALAR(A.data, '$.store._id') AS `_id`,
      JSON_EXTRACT_SCALAR(A.data, '$.store.storeId') AS store_id,
      CAST(JSON_EXTRACT_SCALAR(A.data, '$.store.approvedAt') AS TIMESTAMP) AS approved_at,
      E.apps,
      IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.store.areaId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.store.areaId'), '"', ''))  AS area_id,
      IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.store.assignedTaskId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.store.assignedTaskId'), '"', ''))  AS assigned_task_id,
      JSON_EXTRACT_SCALAR(A.data, '$.store.companyId') AS company_id,
      CAST(JSON_EXTRACT_SCALAR(A.data, '$.store.createdAt') AS TIMESTAMP) AS created_at,
      JSON_EXTRACT_SCALAR(A.data, '$.store.createdBy') AS created_by,
      IF(JSON_EXTRACT_SCALAR(A.data, '$.store.externalId') = "", NULL, JSON_EXTRACT(A.data, '$.store.externalId'))  AS external_id,
      CAST(JSON_EXTRACT_SCALAR(A.data, '$.store.isDeleted') AS BOOL) AS is_deleted,
      C.list_address,
      D.main_address,
      JSON_EXTRACT_SCALAR(A.data, '$.store.mainAddressId') AS main_address_id,
      CAST(JSON_EXTRACT_SCALAR(A.data, '$.store.modifiedAt') AS TIMESTAMP) AS modified_at,
      JSON_EXTRACT_SCALAR(A.data, '$.store.modifiedBy') AS modified_by,
      JSON_EXTRACT_SCALAR(A.data, '$.store.salesEmployeeStatus') AS sales_employee_status,
      JSON_EXTRACT_SCALAR(A.data, '$.store.salesId') AS sales_id,
      JSON_EXTRACT_SCALAR(A.data, '$.store.status') AS status,
      JSON_EXTRACT_SCALAR(A.data, '$.store.storeCode') AS store_code,
      B.store_images,
      G.store_owner,
      JSON_EXTRACT_SCALAR(A.data, '$.store.subAreaId') AS sub_area_id,
      JSON_EXTRACT_SCALAR(A.data, '$.store.userId') AS user_id,
      JSON_EXTRACT_SCALAR(A.data, '$.store.userType') AS user_type,
      F.metadata,
      CAST(JSON_EXTRACT_SCALAR(A.data, '$.store.isActive') AS BOOL) AS is_active,
      JSON_EXTRACT_SCALAR(A.data, '$.store.username') AS username
    ) AS store
  FROM base A
    LEFT JOIN store_images B
    ON A.data = B.data
    AND A.ts = B.published_timestamp

    LEFT JOIN list_address C
    ON A.data = C.data
    AND A.ts = B.published_timestamp

    LEFT JOIN main_address D
    ON A.data = D.data
    AND A.ts = C.published_timestamp

    LEFT JOIN store_apps E
    ON A.data = E.data
    AND A.ts = D.published_timestamp

    LEFT JOIN store_metadata F
    ON A.data = F.data
    AND A.ts = E.published_timestamp

    LEFT JOIN store_owner G
    ON A.data = G.data
    AND A.ts = E.published_timestamp
)

-- END STORE

-- BEGIN POOL_STATUS

,pre_pool_status AS (
  SELECT
    data,
    ts AS published_timestamp,
    REPLACE(REPLACE(pool_status, '\\{"', ''), '}', '') AS pool_status
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data,'$.poolStatus')) AS pool_status
)
,pool_status AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG(
      STRUCT(
        CAST(REPLACE(JSON_EXTRACT(pool_status,'$.dateTime'), '"', '') AS TIMESTAMP) AS date_time,
        REPLACE(JSON_EXTRACT(pool_status,'$.status'), '"', '') AS status,
        CAST(REPLACE(JSON_EXTRACT(pool_status,'$.isChange'), '"', '') AS BOOL) AS is_change
      )
    ) AS pool_status
  FROM
    pre_pool_status
  GROUP BY 1,2
)

-- END POOL_STATUS

-- BEGIN PAYMENT

,payment_procedure_1 AS (
  SELECT 
    data,
    ts AS published_timestamp,
    payment_procedure
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(JSON_EXTRACT(data, '$.payment.paymentProcedure'), '"[{', ''), '}]\\', ''))) AS payment_procedure
)

,payment_procedure_2 AS (
  SELECT
    data,
    published_timestamp,
    IF(REPLACE(JSON_EXTRACT(data, '$.procedureTitle'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.procedureTitle'), '"', '')) AS procedure_title,
    ARRAY_AGG (
      REPLACE(procedure_steps, '"', '')
    ) AS procedure_steps
  FROM payment_procedure_1,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(payment_procedure, '$.procedureSteps'))) AS procedure_steps
  GROUP BY 1,2
)

,payment_procedure AS (
  SELECT
    data,
    published_timestamp,
    ARRAY_AGG (
      STRUCT (
        procedure_title,
        procedure_steps
      )
    ) AS payment_procedure
  FROM
    payment_procedure_2
  GROUP BY 1,2
)

,payment AS (
  SELECT
    A.data,
    ts AS published_timestamp,
    STRUCT (
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment._id'), '"', '') AS _id,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentType'), '"', '') AS payment_type,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentName'), '"', '') AS payment_name,
      B.payment_procedure,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentDescription'), '"', '') AS payment_description,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentId'), '"', '') AS payment_id,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentFee'), '"', '') AS FLOAT64) AS payment_fee,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.isDefault'), '"', '') AS BOOL) AS is_default,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.isDeleted'), '"', '') AS BOOL) AS is_deleted,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.createdBy'), '"', '') AS created_by,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.modifiedBy'), '"', '') AS modified_by,
      IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.companyId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.companyId'), '"', '')) AS company_id,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentLabel	'), '"', '') AS payment_label,
      STRUCT (
        STRUCT (
          CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.paymentMethodFee.nominalFee'), '"', '') AS FLOAT64) AS nominal_fee,
          CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.paymentMethodFee.percentageFee'), '"', '') AS FLOAT64) AS percentage_fee
        ) AS payment_method_fee,
        CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.customerTimeLimit'), '"', '') AS INT64) AS customer_time_limit,
        CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.realTimeLimit'), '"', '') AS INT64) AS real_time_limit,
        STRUCT (
          CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.paymentGatewayFee.nominalFee'), '"', '') AS FLOAT64) AS nominal_fee,
          CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.paymentGatewayFee.percentageFee'), '"', '') AS FLOAT64) AS percentage_fee
        ) AS payment_gateway_fee,
        CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentMetadata.isActive'), '"', '') AS BOOL) AS is_active
      ) AS payment_metadata,
      IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentImage'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentImage'), '"', '')) AS payment_image,
      CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.isActive'), '"', '') AS BOOL) AS is_active,
      REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.payment.paymentIllustration'), '"', '') AS payment_ilustration
    ) AS payment
  FROM
    base A
  LEFT JOIN payment_procedure B
  ON A.data = B.data
  AND A.ts = B.published_timestamp
)

-- END PAYMENT

-- BEGIN ORDER_PRODUCT

,pre_order_product AS (
  SELECT
    data,
    ts AS published_timestamp,
    order_product
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.orderProduct')) AS order_product
  ORDER BY 1
)

,pre_booked_stock AS (
  SELECT
    data,
    published_timestamp,
    ARRAY(
      SELECT STRUCT (
        JSON_EXTRACT_SCALAR(booked_stock, '$.orderId') AS order_id,
        JSON_EXTRACT_SCALAR(booked_stock, '$.stock') AS stock
      )
    ) AS booked_stock
  FROM
    pre_order_product,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(order_product, '$.bookedStock'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS booked_stock
)

,pre_subproduct_images AS (
  SELECT
    data,
    published_timestamp,
    ARRAY(
      SELECT
      REPLACE(sub_product_images, '"', '')
    ) AS sub_product_images
  FROM
    pre_order_product,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(order_product, '$.subProductImages'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS sub_product_images
)

,pre_subproduct_variant AS (
  SELECT
    data,
    published_timestamp,
    ARRAY(
      SELECT STRUCT (
        IF(JSON_EXTRACT_SCALAR(sub_product_variant, '$.variantName') = "", NULL, JSON_EXTRACT_SCALAR(sub_product_variant, '$.variantName')) AS variant_name,
        IF(JSON_EXTRACT_SCALAR(sub_product_variant, '$.variant') = "", NULL, JSON_EXTRACT_SCALAR(sub_product_variant, '$.variant')) AS variant
      )
    ) AS sub_product_variant
  FROM
    pre_order_product,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(order_product, '$.subProductVariant'), '\\', ''), '\"[', '['), ']\"', ']'), '$.')) AS sub_product_variant
)

,order_product AS (
  SELECT
    A.data,
    A.published_timestamp,
    ARRAY_AGG (
      STRUCT (
        REPLACE(JSON_EXTRACT(order_product, '$.subProductId'), '"', '') AS subProductId,
        B.booked_stock AS booked_stock,
        IF(REPLACE(JSON_EXTRACT(order_product, '$.brandId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(order_product, '$.brandId'), '"', '')) AS brand_id,
        STRUCT (
          REPLACE(JSON_EXTRACT(order_product, '$.categoryIds.categoryId'), '"', '') AS category_id,
          REPLACE(JSON_EXTRACT(order_product, '$.categoryIds.subCategoryId'), '"', '') AS sub_category_id,
          REPLACE(JSON_EXTRACT(order_product, '$.categoryIds.subSubCategoryId'), '"', '') AS sub_sub_category_id
        ) AS category_ids,
        REPLACE(JSON_EXTRACT(order_product, '$.companyId'), '"', '') AS company_id,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
        REPLACE(JSON_EXTRACT(order_product, '$.createdBy'), '"', '') AS created_by,
        IF(REPLACE(JSON_EXTRACT(order_product, '$.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(order_product, '$.externalId'), '"', '')) AS external_id,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.isBonus'), '"', '') AS BOOL) AS is_bonus,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.isTax'), '"', '') AS BOOL) AS is_tax,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
        REPLACE(JSON_EXTRACT(order_product, '$.modifiedBy'), '"', '') AS modified_by,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.onShelf'), '"', '') AS BOOL) AS on_shelf,
        REPLACE(JSON_EXTRACT(order_product, '$.productId'), '"', '') AS product_id,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.productOnShelf'), '"', '') AS BOOL) AS product_on_shelf,
        REPLACE(JSON_EXTRACT(order_product, '$.subProductDescription'), '"', '') AS sub_product_description,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductDiscountPercent'), '"', '') AS FLOAT64) AS sub_product_discount_percent,
        C.sub_product_images AS sub_product_images,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductMinimumOrder'), '"', '') AS INT64) AS sub_product_minimum_order,
        REPLACE(JSON_EXTRACT(order_product, '$.subProductName'), '"', '') AS sub_product_name,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductPrice'), '"', '') AS INT64) AS sub_product_price,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductStock'), '"', '') AS INT64) AS sub_product_stock,
        REPLACE(JSON_EXTRACT(order_product, '$.subProductUnit'), '"', '') AS sub_product_unit,
        D.sub_product_variant AS sub_product_variant,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductWeight'), '"', '') AS INT64) AS sub_product_weight,
        IF(REPLACE(JSON_EXTRACT(order_product, '$.subProductsSize'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(order_product, '$.subProductsSize'), '"', '')) AS sub_product_size,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductStockOnHold'), '"', '') AS INT64) AS sub_product_stock_on_hold,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.stockModifiedTimeStamp'), '"', '') AS TIMESTAMP) AS stock_modified_timestamp,
        IF(REPLACE(JSON_EXTRACT(order_product, '$.subProductOrderStockStatus'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(order_product, '$.subProductOrderStockStatus'), '"', '')) AS sub_product_minimum_order_stock_status,
        IF(REPLACE(JSON_EXTRACT(order_product, '$.subProductMinimumOrderStatus'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(order_product, '$.subProductMinimumOrderStatus'), '"', '')) AS sub_product_minimum_order_status,
        REPLACE(JSON_EXTRACT(order_product, '$.productName'), '"', '') AS product_name,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductDiscountAmount'), '"', '') AS INT64) AS sub_product_discount_amount,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductTotalDiscountAmount'), '"', '') AS INT64) AS sub_product_total_discount_amount,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductDiscountPrice'), '"', '') AS INT64) AS sub_product_discount_price,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductTotalDiscountPrice'), '"', '') AS INT64) AS sub_product_total_discount_price,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductOrderAmount'), '"', '') AS INT64) AS sub_product_order_amount,
        CAST(REPLACE(JSON_EXTRACT(order_product, '$.subProductTotalPrice'), '"', '') AS INT64) AS sub_product_total_price,
        IF(REPLACE(JSON_EXTRACT(order_product, '$.subProductOrderNotes'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(order_product, '$.subProductOrderNotes'), '"', '')) AS sub_product_order_notes
      )
    ) AS order_product
  FROM
    pre_order_product A
    LEFT JOIN pre_booked_stock B
    ON A.data = B.data
    AND A.published_timestamp = B.published_timestamp

    LEFT JOIN pre_subproduct_images C
    ON A.data = C.data
    AND A.published_timestamp = C.published_timestamp

    LEFT JOIN pre_subproduct_variant D
    ON A.data = D.data
    AND A.published_timestamp = D.published_timestamp
  GROUP BY 1,2
)

-- END ORDER_PRODUCT

SELECT 
  JSON_EXTRACT_SCALAR(A.data, '$.orderId') AS order_id,
  JSON_EXTRACT_SCALAR(A.data, '$.invoiceId') AS invoice_id,
  JSON_EXTRACT_SCALAR(A.data, '$.companyId') AS company_id,
  JSON_EXTRACT_SCALAR(A.data, '$.storeId') AS store_id,
  JSON_EXTRACT_SCALAR(A.data, '$.billId') AS bill_id,
  JSON_EXTRACT_SCALAR(A.data, '$.salesOrderId') AS sales_order_id,
  JSON_EXTRACT_SCALAR(A.data, '$.paymentId') AS payment_id,
  JSON_EXTRACT_SCALAR(A.data, '$.purchaseOnDeliveryId') AS purchase_on_delivery_id,
  JSON_EXTRACT_SCALAR(A.data, '$.purchaseOrderId') AS purchase_order_id,
  IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.urlPurchaseOnDeliveryPdf'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.urlPurchaseOnDeliveryPdf'), '"', '')) AS url_purchase_on_delivery_pdf,
  JSON_EXTRACT_SCALAR(A.data, '$.orderMethod') AS order_method,
  JSON_EXTRACT_SCALAR(A.data, '$.paymentMethodLabel') AS payment_method_label,
  JSON_EXTRACT_SCALAR(A.data, '$.companyCategory') AS company_category,
  JSON_EXTRACT_SCALAR(A.data, '$.urlSalesOrderPdf') AS url_sales_order_pdf,
  JSON_EXTRACT_SCALAR(A.data, '$.companyName') AS company_name,
  JSON_EXTRACT_SCALAR(A.data, '$.billCode') AS bill_code,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.orderDiscount') AS INT64) AS order_discount,
  JSON_EXTRACT_SCALAR(A.data, '$.orderStatus') AS order_status,
  IF(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.urlInvoicePdf'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.urlInvoicePdf'), '"', ''))  AS url_invoice_pdf,
  CAST(JSON_EXTRACT_SCALAR(A.data, '$.isFulfillmentProcess') AS BOOL) AS is_fullfilment_process,
  JSON_EXTRACT_SCALAR(A.data, '$.urlPurchaseOrderPdf') AS url_purchase_order_pdf,
  JSON_EXTRACT_SCALAR(A.data, '$.orderCode') AS order_code,
  JSON_EXTRACT_SCALAR(A.data, '$.paymentMethod') AS payment_method,
  B.store,
  C.pool_status,
  STRUCT(
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalPrice'), '"', '') AS FLOAT64) AS total_price,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalDiscount'), '"', '') AS FLOAT64) AS total_discount,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalVoucherAmount'), '"', '') AS FLOAT64) AS total_voucher_amount,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalUsedVoucherAmount'), '"', '') AS FLOAT64) AS total_used_voucher_amount,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalProductDiscount'), '"', '') AS FLOAT64) AS total_product_discount,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalInpoinCoin'), '"', '') AS FLOAT64) AS total_inpoin_coin,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalAdditionalDiscount'), '"', '') AS FLOAT64) AS total_additional_discount,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalAdditionalFee'), '"', '') AS FLOAT64) AS total_additional_fee,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalAdditionalCharge'), '"', '') AS FLOAT64) AS total_additional_charge,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalFee'), '"', '') AS FLOAT64) AS total_fee,
    CAST(REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderSummary.totalPayment'), '"', '') AS FLOAT64) AS total_payment
  ) AS order_summary,
  STRUCT(
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderBy.storeId'), '"', '') AS store_id,
    REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderBy.salesId'), '"', '') AS sales_id
  ) AS order_by,
  STRUCT (
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.storeName'), '"', '') AS store_name,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.recipientName'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT_SCALAR(A.data, '$.orderAddress.recipientName'), '"', ''))  AS recipient_name,
    CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.phoneNumber'), '"', '') AS INT64) AS phone_number,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.province	'), '"', '') AS province,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.provinceId'), '"', '') AS province_id,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.cityId'), '"', '') AS city_id,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.district'), '"', '') AS district,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.districtId'), '"', '') AS district_id,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.subDistrict'), '"', '') AS sub_district,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.subDistrictId'), '"', '') AS sub_district_id,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.zipCode'), '"', '') AS zip_code,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.zipCodeId'), '"', '') AS zip_code_id,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.address'), '"', '') AS address,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.mainAddress'), '"', '') AS main_address,
    IF(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.externalId'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.externalId'), '"', '')) AS external_id,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.lat'), '"', '') AS lat,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.long'), '"', '') AS long,
    CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.isFulfillmentProcess'), '"', '') AS BOOL) AS is_fulfillment_process,
    REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.addressId'), '"', '') AS address_id,
    IF(JSON_EXTRACT(A.data, '$.orderAddress.addressMark') = "", NULL, JSON_EXTRACT(A.data, '$.orderAddress.addressMark')) AS address_mark
  ) AS order_address,
  D.payment,
  E.order_product,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isReceived'), '"', '') AS BOOL) AS is_received,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
  REPLACE(JSON_EXTRACT(A.data, '$.createdBy'), '"', '') AS created_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
  REPLACE(JSON_EXTRACT(A.data, '$.modifiedBy'), '"', '') AS modified_by,
  CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
  A.data AS original_data,
  A.ts AS published_timestamp
FROM base A
  LEFT JOIN store B
  ON A.data = B.data
  AND A.ts = B.published_timestamp
  LEFT JOIN pool_status C
  ON A.data = C.data
  AND A.ts = C.published_timestamp
  LEFT JOIN payment D
  ON A.data = D.data
  AND A.ts = D.published_timestamp
  LEFT JOIN order_product E
  ON A.data = E.data
  AND A.ts = E.published_timestamp