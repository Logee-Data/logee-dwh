WITH base AS (
  SELECT
    *
  FROM `logee-data-prod.analytics_236272400.events_{{ ds_nodash }}`
  WHERE event_name = 'view_product'
)

,with_ga_session_id AS (
  SELECT
    base.*,
    event_params_unnested.value.int_value AS ga_session_id
  FROM
    base,
    UNNEST(event_params) event_params_unnested
  WHERE event_params_unnested.key = 'ga_session_id'
)

-- EXTRACT EVENT_PARAMS
,event_params_unnested AS (
  SELECT
    with_ga_session_id.* EXCEPT(event_params),
    event_params.key,
    event_params.value
  FROM
    with_ga_session_id,
    UNNEST(event_params) event_params
)

, product_id AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        ga_session_id,
        value.string_value AS product_id
    FROM
        event_params_unnested
    WHERE
        key = 'product_id'
)
, firebase_screen_id AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        ga_session_id,
        value.int_value AS firebase_screen_id
    FROM
        event_params_unnested
    WHERE
        key = 'firebase_screen_id'
)
, ga_session_number AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        ga_session_id,
        value.int_value AS ga_session_number
    FROM
        event_params_unnested
    WHERE
        key = 'ga_session_number'
)
, firebase_event_origin AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        ga_session_id,
        value.string_value AS firebase_event_origin
    FROM
        event_params_unnested
    WHERE
        key = 'firebase_event_origin'
)
, engaged_session_event AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        ga_session_id,
        value.int_value AS engaged_session_event
    FROM
        event_params_unnested
    WHERE
        key = 'engaged_session_event'
)
, firebase_screen_class AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        ga_session_id,
        value.string_value AS firebase_screen_class
    FROM
        event_params_unnested
    WHERE
        key = 'firebase_screen_class'
)

,event_params AS (
  SELECT
    base.* EXCEPT(event_params),
    product_id.product_id,
    firebase_screen_id.firebase_screen_id,
    ga_session_number.ga_session_number,
    firebase_event_origin.firebase_event_origin,
    engaged_session_event.engaged_session_event,
    firebase_screen_class.firebase_screen_class
  FROM
    with_ga_session_id base

  LEFT JOIN product_id
      ON base.user_pseudo_id = product_id.user_pseudo_id
      AND base.event_timestamp = product_id.event_timestamp
      AND base.ga_session_id = product_id.ga_session_id
  LEFT JOIN firebase_screen_id
      ON base.user_pseudo_id = firebase_screen_id.user_pseudo_id
      AND base.event_timestamp = firebase_screen_id.event_timestamp
      AND base.ga_session_id = firebase_screen_id.ga_session_id
  LEFT JOIN ga_session_number
      ON base.user_pseudo_id = ga_session_number.user_pseudo_id
      AND base.event_timestamp = ga_session_number.event_timestamp
      AND base.ga_session_id = ga_session_number.ga_session_id
  LEFT JOIN firebase_event_origin
      ON base.user_pseudo_id = firebase_event_origin.user_pseudo_id
      AND base.event_timestamp = firebase_event_origin.event_timestamp
      AND base.ga_session_id = firebase_event_origin.ga_session_id
  LEFT JOIN engaged_session_event
      ON base.user_pseudo_id = engaged_session_event.user_pseudo_id
      AND base.event_timestamp = engaged_session_event.event_timestamp
      AND base.ga_session_id = engaged_session_event.ga_session_id
  LEFT JOIN firebase_screen_class
      ON base.user_pseudo_id = firebase_screen_class.user_pseudo_id
      AND base.event_timestamp = firebase_screen_class.event_timestamp
      AND base.ga_session_id = firebase_screen_class.ga_session_id
)

-- GET USER_PROPERTIES.FIRST_OPEN_TIME
,first_open_time AS (
  SELECT
    base.* EXCEPT(user_properties),
    user_properties.value.int_value AS first_open_time
  FROM
    event_params base,
    UNNEST(user_properties) user_properties
  WHERE user_properties.key = 'first_open_time'
)

,final AS (
  SELECT
    * EXCEPT(
      user_ltv, ecommerce, items, event_value_in_usd
    ) REPLACE(
      PARSE_DATE("%Y%m%d", event_date) AS event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
      TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
      CAST(ga_session_id AS STRING) AS ga_session_id,
      CAST(firebase_screen_id AS STRING) AS firebase_screen_id,
      TIMESTAMP_MILLIS(first_open_time.first_open_time) AS first_open_time
    )
  FROM first_open_time
)

SELECT * FROM final
