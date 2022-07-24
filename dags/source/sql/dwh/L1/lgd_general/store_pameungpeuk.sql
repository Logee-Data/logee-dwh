with base as (SELECT
  szCustId AS customer_id,
  szName AS customer_name,
  CustszContactPerson AS customer_contact_person,
  CustszPhoneNo_1 as customer_contact_number,
  CustszEmail as customer_email,
  INITCAP(CustszBuilding) AS customer_building,
  INITCAP(CustszAddress_1) AS customer_address_1,
  INITCAP(CustszAddress_2) AS customer_address_2,
  LocationszZipcode as customer_zipcode,
  szLatitude AS address_latitude,
  szLongitude AS address_longitude,
  szSalesId as sales_id,
  szSalesNm as sales_name,
  DATE(PARSE_DATETIME("%d-%b-%Y %H:%M AM", dtmStart)) AS contract_begin_at
FROM `logee-data-prod.stg_sheets.store_pameungpeuk`)

SELECT
  *,
  CURRENT_TIMESTAMP() AS published_timestamp
FROM
  base
