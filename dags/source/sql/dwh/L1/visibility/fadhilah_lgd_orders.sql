WITH

base AS (
  SELECT * 
  FROM 
    `logee-data-prod.logee_datalake_raw_production.visibility_lgd_orders` 
  WHERE
    _date_partition = "2022-07-11"
)

-----------------------------------------------------------------------------------------------------------
----BEGIN payment

-- BEGIN paymentProcedure
,pre_payment_procedure_1 AS (
  SELECT
    data,
    ts AS published_timestamp,
    payment_procedure
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE(JSON_EXTRACT(data, '$.payment.paymentProcedure'), '"[{', ''), '}]\\', ''))) AS payment_procedure
)

,pre_payment_procedure_2 AS (
  SELECT
    data,
    published_timestamp,
    IF(REPLACE(JSON_EXTRACT(data, '$.procedureTitle'), '"', '') = "", NULL, REPLACE(JSON_EXTRACT(data, '$.procedureTitle'), '"', '')) AS procedure_title,
    ARRAY_AGG (
      REPLACE(procedure_steps, '"', '')
    ) AS procedure_steps
  FROM
    pre_payment_procedure_1,
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
    pre_payment_procedure_2
  GROUP BY 1,2
)
--end paymentProcedure



---Begin payment
, payment AS (
  SELECT
    A.data,
    ts AS published_timestamp,
      STRUCT (
		REPLACE(JSON_EXTRACT(A.data, '$.payment._id'), '"', '') AS _id,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentType'), '"', '') AS payment_type,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentName'), '"', '') AS payment_name,
		H.payment_procedure,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentDescription'), '"', '') AS payment_description,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentId'), '"', '') AS payment_id,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentFee'), '"', '') AS payment_fee,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.payment.isDefault'), '"', '') AS BOOL) AS is_default,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.payment.isDeleted'), '"', '') AS BOOL) AS is_deleted,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.payment.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.createdBy'), '"', '') AS created_by,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.payment.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.modifiedBy'), '"', '') AS modified_by,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.companyId'), '"', '') AS company_id,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentLabel'), '"', '') AS payment_label,
		------paymentMetadata
		STRUCT (
			-----paymentMethodFee
			STRUCT (
				REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.paymentMethodFee.nominalFee'), '"', '') AS nominal_fee,
				REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.paymentMethodFee.percentageFee'), '"', '') AS percentage_fee
			) AS payment_method_fee,
			REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.customerTimeLimit'), '"', '') AS customer_time_limit,
			REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.realTimeLimit'), '"', '') AS real_time_limit,
			-----paymentGatewayFee
			STRUCT (
				REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.paymentGatewayFee.nominalFee'), '"', '') AS nominal_fee,
				REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.paymentGatewayFee.percentageFee'), '"', '') AS percentage_fee
			) AS payment_gateway_fee,
			CAST(REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentMetadata.isActive'), '"', '') AS BOOL) AS is_active
		) AS payment_metadata,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentImage'), '"', '') AS payment_image,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.payment.isActive'), '"', '') AS BOOL) AS is_active,
		REPLACE(JSON_EXTRACT(A.data, '$.payment.paymentIllustration'), '"', '') AS payment_illustration
      )  AS payment
  FROM base A
    LEFT JOIN payment_procedure H
    ON A.data = H.data
    AND A.ts = H.published_timestamp
	
)
--end payment


-----------------------------------------------------------------------------------------------------------
---BEGIN store

-- BEGIN apps
,apps AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      DISTINCT REPLACE(apps, '"', '')
    ) AS apps
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.store.apps')) AS apps
  GROUP BY 1,2
)
-- END apps

--Begin listAddress
, list_address AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT (
        IF(JSON_EXTRACT_SCALAR(list_address, '$.storeName') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.storeName')) AS store_name,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.recipientName') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.recipientName')) AS recipient_name,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.phoneNumber') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.phoneNumber')) AS phone_number,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.province') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.province')) AS province,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.provinceId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.provinceId')) AS province_id,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.city') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.city')) AS city,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.cityId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.cityId')) AS city_id,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.district') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.district')) AS district,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.districtId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.districtId')) AS district_id,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.subDistrict') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.subDistrict')) AS sub_district,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.subDistrictId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.subDistrictId')) AS sub_district_id,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.zipCode') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.zipCode')) AS zip_code,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.zipCodeId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.zipCodeId')) AS zip_code_id,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.address') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.address')) AS address,
		CAST(IF(JSON_EXTRACT_SCALAR(list_address, '$.mainAddress') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.mainAddress')) AS BOOL) AS main_address,
		CAST(IF(JSON_EXTRACT_SCALAR(list_address, '$.isFulfillmentProcess') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.isFulfillmentProcess')) AS BOOL) AS is_fulfillment_process,
		IF(JSON_EXTRACT_SCALAR(list_address, '$.addressId') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.addressId')) AS address_id,
		CAST(IF(JSON_EXTRACT_SCALAR(list_address, '$.lat') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.lat')) AS FLOAT64) AS lat,
		CAST(IF(JSON_EXTRACT_SCALAR(list_address, '$.long') = "", NULL, JSON_EXTRACT_SCALAR(list_address, '$.long')) AS FLOAT64) AS long
      )
    ) AS list_address
  FROM
    base,
	UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.store'), '$.listAddress')) AS list_address
  GROUP BY 1,2
)
--end listAddress



--Begin store_images
, store_images AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT (
        IF(JSON_EXTRACT_SCALAR(store_images, '$.image') = "", NULL, JSON_EXTRACT_SCALAR(store_images, '$.image')) AS image,
		CAST(IF(JSON_EXTRACT_SCALAR(store_images, '$.mainImage') = "", NULL, JSON_EXTRACT_SCALAR(store_images, '$.mainImage')) AS BOOL) AS main_image
      )
    ) AS store_images
  FROM
    base,
	UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.store'), '$.storeImages')) AS store_images
  GROUP BY 1,2
)
--end store_images



---Begin store
, store AS (
  SELECT
    A.data,
    ts AS published_timestamp,
      STRUCT (
		REPLACE(JSON_EXTRACT(A.data, '$.store._id'), '"', '') AS _id,
		-----storeOwner
		STRUCT (
			REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerName'), '"', '') AS owner_name,
			REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerPhoneNumber'), '"', '') AS owner_phone_number,
			-----ownerAddress
			STRUCT (
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.province'), '"', '') AS province,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.provinceId'), '"', '') AS province_id,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.city'), '"', '') AS city,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.cityId'), '"', '') AS city_id,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.address'), '"', '') AS address,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.district'), '"', '') AS district,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.districtId'), '"', '') AS district_id,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.subDistrict'), '"', '') AS sub_district,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.subDistrictId'), '"', '') AS sub_district_id,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.zipCode'), '"', '') AS zip_code,
				REPLACE(JSON_EXTRACT(A.data, '$.store.storeOwner.ownerAddress.zipCodeId'), '"', '') AS zip_code_id
			) AS owner_address
		) AS store_owner,
		REPLACE(JSON_EXTRACT(A.data, '$.store.areaId'), '"', '') AS area_id,
		REPLACE(JSON_EXTRACT(A.data, '$.store.subAreaId'), '"', '') AS sub_area_id,
		REPLACE(JSON_EXTRACT(A.data, '$.store.salesId'), '"', '') AS sales_id,
		REPLACE(JSON_EXTRACT(A.data, '$.store.companyId'), '"', '') AS company_id,
		REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddressId'), '"', '') AS main_address_id,
		-----mainAddress
		STRUCT (
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.storeName'), '"', '') AS store_name,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.recipientName'), '"', '') AS recipient_name,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.phoneNumber'), '"', '') AS phone_number,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.province'), '"', '') AS province,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.provinceId'), '"', '') AS province_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.city'), '"', '') AS city,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.cityId'), '"', '') AS city_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.district'), '"', '') AS district,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.districtId'), '"', '') AS district_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.subDistrict'), '"', '') AS sub_district,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.subDistrictId'), '"', '') AS sub_district_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.zipCode'), '"', '') AS zip_code,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.zipCodeId'), '"', '') AS zip_code_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.address'), '"', '') AS address,
			CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.mainAddress'), '"', '') AS BOOL) AS main_address,
			CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.isFulfillmentProcess'), '"', '') AS BOOL) AS is_fulfillment_process,
			CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.lat'), '"', '') AS FLOAT64) AS lat,
			CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.mainAddress.long'), '"', '') AS FLOAT64) AS long
		) AS main_address,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.isDeleted'), '"', '') AS BOOL) AS is_deleted,
		REPLACE(JSON_EXTRACT(A.data, '$.store.storeId'), '"', '') AS store_id,
		REPLACE(JSON_EXTRACT(A.data, '$.store.userId'), '"', '') AS user_id,
		----list_address
		D.list_address,
		----apps
		E.apps,
		REPLACE(JSON_EXTRACT(A.data, '$.store.userType'), '"', '') AS user_type,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
		REPLACE(JSON_EXTRACT(A.data, '$.store.createdBy'), '"', '') AS created_by,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.store.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
		REPLACE(JSON_EXTRACT(A.data, '$.store.modifiedBy'), '"', '') AS modified_by,
		REPLACE(JSON_EXTRACT(A.data, '$.store.storeImage'), '"', '') AS store_image,
		REPLACE(JSON_EXTRACT(A.data, '$.store.assignedTaskId'), '"', '') AS assigned_task_id,
		REPLACE(JSON_EXTRACT(A.data, '$.store.status'), '"', '') AS status,
		REPLACE(JSON_EXTRACT(A.data, '$.store.approvedAt'), '"', '') AS approved_at,
		-----metadata
		STRUCT (
			REPLACE(JSON_EXTRACT(A.data, '$.store.metadata.firstOrderId'), '"', '') AS first_order_id
		) AS metadata,
		REPLACE(JSON_EXTRACT(A.data, '$.store.storeCode'), '"', '') AS store_code,
		----store_images
		F.store_images
      ) AS store
  FROM base A
    LEFT JOIN list_address D
    ON A.data = D.data
    AND A.ts = D.published_timestamp
	
	LEFT JOIN apps E
    ON A.data = E.data
    AND A.ts = E.published_timestamp
	
	LEFT JOIN store_images F
    ON A.data = F.data
    AND A.ts = F.published_timestamp
)
--end store
-----------------------------------------------------------------------------------------------------
--Begin PoolStatus
, pool_status AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT (
        CAST(IF(JSON_EXTRACT_SCALAR(pool_status, '$.dateTime') = "", NULL, JSON_EXTRACT_SCALAR(pool_status, '$.dateTime')) AS TIMESTAMP) AS date_time,
        IF(JSON_EXTRACT_SCALAR(pool_status, '$.status') = "", NULL, JSON_EXTRACT_SCALAR(pool_status, '$.status')) AS status
      )
    ) AS pool_status
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.poolStatus')) AS pool_status
  GROUP BY 1,2
)
--end PoolStatus
----------------------------------------------------------------------------------------------------

----BEGIN order_product

-- BEGIN subProductImages
,sub_product_images AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG (
      DISTINCT REPLACE(sub_product_images, '"', '')
    ) AS sub_product_images
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.orderProduct.subProductImages')) AS sub_product_images
  GROUP BY 1,2
)
-- END sub_product_images



--Begin subProductVariant
, sub_product_variant AS (
  SELECT
    data,
    ts AS published_timestamp,
    ARRAY_AGG(
      STRUCT (
        IF(JSON_EXTRACT_SCALAR(sub_product_variant, '$.orderProduct.variantName') = "", NULL, JSON_EXTRACT_SCALAR(sub_product_variant, '$.orderProduct.variantName')) AS variant_name,
		IF(JSON_EXTRACT_SCALAR(sub_product_variant, '$.orderProduct.variant') = "", NULL, JSON_EXTRACT_SCALAR(sub_product_variant, '$.orderProduct.variant')) AS variant
      )
    ) AS sub_product_variant
  FROM
    base,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.subProductVariant')) AS sub_product_variant
  GROUP BY 1,2
)
--end subProductVariant


-- BEGIN bookedStock
,pre_booked_stock AS (
  SELECT
  data,
  ts AS published_timestamp,
  STRUCT(
     REPLACE(JSON_EXTRACT(booked_stock, '$.orderId'), '"', '') AS order_id,
     REPLACE(JSON_EXTRACT(booked_stock, '$.stock'), '"', '') AS type
  ) AS booked_stock
  FROM base,
   UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.orderProduct'), '$.bookedStock')) AS booked_stock
)

,booked_stock AS (
  SELECT
  data,
  published_timestamp,
  ARRAY_AGG(booked_stock) AS booked_stock
  from pre_booked_stock
  GROUP BY 1, 2
)
-- END


---Begin order_product
, order_product AS (
  SELECT
    A.data,
    ts AS published_timestamp,
	ARRAY_AGG(
      STRUCT (
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.companyId'), '"', '') AS company_id,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductId'), '"', '') AS sub_product_id,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.brandId'), '"', '') AS brand_id,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.categoryId'), '"', '') AS category_id,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.productId'), '"', '') AS product_id,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductName'), '"', '') AS sub_product_name,
		K.sub_product_images,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductSize'), '"', '') AS sub_product_size,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductStock'), '"', '') AS sub_product_stock,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductDiscountPercent'), '"', '') AS sub_product_discount_percent,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductDescription'), '"', '') AS sub_product_description,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductUnit'), '"', '') AS sub_product_unit,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductPrice'), '"', '') AS sub_product_price,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductWeight'), '"', '') AS sub_product_weight,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.isDeleted'), '"', '') AS BOOL) AS is_deleted,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.createdBy'), '"', '') AS created_by,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.onShelf'), '"', '') AS modified_by,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.productOnShelf'), '"', '') AS external_id,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.isDeleted'), '"', '') AS BOOL) AS on_shelf,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.createdAt'), '"', '') AS BOOL) AS product_on_shelf,
		L.sub_product_variant,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductsSize'), '"', '') AS sub_products_size,
		M.booked_stock,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.isTax'), '"', '') AS is_tax,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductMinimumOrder'), '"', '') AS sub_product_minimum_order,
		-----categoryIds
		STRUCT (
			REPLACE(JSON_EXTRACT(A.data, '$.store.categoryIds.categoryId'), '"', '') AS category_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.categoryIds.subCategoryId'), '"', '') AS sub_category_id,
			REPLACE(JSON_EXTRACT(A.data, '$.store.categoryIds.subSubCategoryId'), '"', '') AS sub_sub_category_id
		) AS category_ids,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductStockOnHold'), '"', '') AS sub_product_stock_on_hold,
		CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.isBonus'), '"', '') AS BOOL) AS is_bonus,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductOrderStockStatus'), '"', '') AS sub_product_order_stock_status,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductMinimumOrderStatus'), '"', '') AS sub_product_minimum_order_status,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.productName'), '"', '') AS product_name,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductDiscountAmount'), '"', '') AS sub_product_discount_amount,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductTotalDiscountAmount'), '"', '') AS sub_product_total_discount_amount,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductDiscountPrice'), '"', '') AS sub_product_discount_price,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductTotalDiscountPrice'), '"', '') AS sub_product_total_discount_price,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductOrderAmount'), '"', '') AS sub_product_order_amount,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductTotalPrice'), '"', '') AS sub_product_total_price,
		REPLACE(JSON_EXTRACT(A.data, '$.orderProduct.subProductOrderNotes'), '"', '') AS sub_product_order_notes
      ) 
	) AS order_product
  FROM base A
    LEFT JOIN sub_product_images K
    ON A.data = K.data
    AND A.ts = K.published_timestamp

	LEFT JOIN sub_product_variant L
    ON A.data = L.data
    AND A.ts = L.published_timestamp

	LEFT JOIN booked_stock M
    ON A.data = M.data
    AND A.ts = M.published_timestamp,
    UNNEST(JSON_EXTRACT_ARRAY(A.data, '$.orderProduct')) AS order_product
  GROUP BY 1,2
)
--end order_product
--------------------------------------------------------------------------------------------------

------BEGIN MAIN


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
JSON_EXTRACT_SCALAR(A.data, '$.urlPurchaseOnDeliveryPdf') AS url_purchase_on_delivery_pdf,
JSON_EXTRACT_SCALAR(A.data, '$.orderMethod') AS order_method,
JSON_EXTRACT_SCALAR(A.data, '$.paymentMethodLabel') AS payment_method_label,
JSON_EXTRACT_SCALAR(A.data, '$.companyCategory') AS company_category,
JSON_EXTRACT_SCALAR(A.data, '$.urlSalesOrderPdf') AS url_sales_order_pdf,
JSON_EXTRACT_SCALAR(A.data, '$.companyName') AS company_name,
JSON_EXTRACT_SCALAR(A.data, '$.billCode') AS bill_code,
JSON_EXTRACT_SCALAR(A.data, '$.orderDiscount') AS order_discount,
JSON_EXTRACT_SCALAR(A.data, '$.orderStatus') AS order_status,
JSON_EXTRACT_SCALAR(A.data, '$.urlInvoicePdf') AS url_invoice_pdf,
JSON_EXTRACT_SCALAR(A.data, '$.isFulfillmentProcess') AS is_fulfillment_process,
JSON_EXTRACT_SCALAR(A.data, '$.urlPurchaseOrderPdf') AS url_purchase_order_pdf,
JSON_EXTRACT_SCALAR(A.data, '$.orderCode') AS order_code,
JSON_EXTRACT_SCALAR(A.data, '$.paymentMethod') AS payment_method,
-----OrderSummary
STRUCT (
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalPrice'), '"', '') AS FLOAT64) AS total_price,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalDiscount'), '"', '') AS FLOAT64) AS total_discount,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalVoucherAmount'), '"', '') AS FLOAT64) AS total_voucher_amount,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalUsedVoucherAmount'), '"', '') AS FLOAT64) AS total_used_voucher_amount,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalProductDiscount'), '"', '') AS FLOAT64) AS total_product_discount,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalInpoinCoin'), '"', '') AS FLOAT64) AS total_inpoin_coin,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalAdditionalDiscount'), '"', '') AS FLOAT64) AS total_additional_discount,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalAdditionalFee'), '"', '') AS FLOAT64) AS total_additional_fee,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalAdditionalCharge'), '"', '') AS FLOAT64) AS total_additional_charge,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalFee'), '"', '') AS FLOAT64) AS total_fee,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderSummary.totalPayment'), '"', '') AS FLOAT64) AS total_payment
) AS order_summary,
----store
C.store,
----PoolStatus
B.pool_status,
-----OrderAddres
STRUCT (
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.province'), '"', '') AS province,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.provinceId'), '"', '') AS province_id,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.address'), '"', '') AS address,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.storeName'), '"', '') AS store_name,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.recipientName'), '"', '') AS recipient_name,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.phoneNumber'), '"', '') AS phone_number,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.city'), '"', '') AS city,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.cityId'), '"', '') AS city_id,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.district'), '"', '') AS district,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.districtId'), '"', '') AS district_id,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.subDistrict'), '"', '') AS sub_district,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.subDistrictId'), '"', '') AS sub_district_id,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.zipCode'), '"', '') AS zip_code,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.zipCodeId'), '"', '') AS zip_code_id,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.mainAddress'), '"', '') AS main_address,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.externalId'), '"', '') AS external_id,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.lat'), '"', '') AS FLOAT64) AS lat,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.long'), '"', '') AS FLOAT64) AS long,
	CAST(REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.isFulfillmentProcess'), '"', '') AS BOOL) AS is_fulfillment_process,
	REPLACE(JSON_EXTRACT(A.data, '$.orderAddress.addressId'), '"', '') AS address_id
) AS order_address,
----payment
G.payment,
----OrderBy
STRUCT (
	REPLACE(JSON_EXTRACT(A.data, '$.orderBy.storeId'), '"', '') AS store_id
) AS order_by,
J.order_product,
CAST(REPLACE(JSON_EXTRACT(A.data, '$.isReceived'), '"', '') AS BOOL) AS is_received,
CAST(REPLACE(JSON_EXTRACT(A.data, '$.isDeleted'), '"', '') AS BOOL) AS is_deleted,
JSON_EXTRACT_SCALAR(A.data, '$.createdBy') AS created_by,
CAST(REPLACE(JSON_EXTRACT(A.data, '$.createdAt'), '"', '') AS TIMESTAMP) AS created_at,
JSON_EXTRACT_SCALAR(A.data, '$.modifiedBy') AS modified_by,
CAST(REPLACE(JSON_EXTRACT(A.data, '$.modifiedAt'), '"', '') AS TIMESTAMP) AS modified_at,

	A.data AS original_data,
	A.ts

	FROM base A
	LEFT JOIN pool_status B
	ON A.data = B.data
	AND A.ts = B.published_timestamp
	
	LEFT JOIN store C
	ON A.data = C.data
	AND A.ts = C.published_timestamp

	LEFT JOIN payment G
	ON A.data = G.data
	AND A.ts = G.published_timestamp

	LEFT JOIN order_product J
	ON A.data = J.data
	AND A.ts = J.published_timestamp