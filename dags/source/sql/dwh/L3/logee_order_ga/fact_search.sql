WITH raw_base AS (
  SELECT
    *
  FROM `logee-data-prod.analytics_236272400.events_{{ ds_nodash }}`
  WHERE event_name = 'submit_search_term'
)

-- GET GA_SESSION_ID
,ga_session_id AS (
  SELECT
    raw_base.*,
    event_params_unnested.value.int_value AS ga_session_id,
  FROM
    raw_base,
    UNNEST(event_params) event_params_unnested
  WHERE
    event_params_unnested.key = 'ga_session_id'
)

-- GET EVENT_PARAMS
,unnest_event_params AS (
  SELECT
    ga_session_id.*,
    event_params_unnested.value,
    event_params_unnested.key
  FROM
    ga_session_id,
    UNNEST(event_params) event_params_unnested
  WHERE
    event_params_unnested.key != 'ga_session_id'
)

,firebase_screen_class AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    value.string_value AS firebase_screen_class
  FROM
    unnest_event_params
  WHERE
    key = 'firebase_screen_class'
)

,firebase_event_origin AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    value.string_value AS firebase_event_origin
  FROM
    unnest_event_params
  WHERE
    key = 'firebase_event_origin'
)

,ga_session_number AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    value.int_value AS ga_session_number
  FROM
    unnest_event_params
  WHERE
    key = 'ga_session_number'
)

,firebase_screen_id AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    value.string_value AS firebase_screen_id
  FROM
    unnest_event_params
  WHERE
    key = 'firebase_screen_id'
)

-- ,ga_session_id AS (
--   SELECT
--     event_timestamp,
--     user_pseudo_id,
--     value.string_value AS ga_session_id
--   FROM
--     unnest_event_params
--   WHERE
--     key = 'ga_session_id'
-- )

,engaged_session_event AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    value.int_value AS engaged_session_event
  FROM
    unnest_event_params
  WHERE
    key = 'engaged_session_event'
)

,search_term AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    value.string_value AS search_term
  FROM
    unnest_event_params
  WHERE
    key = 'search_term'
)

,event_params AS (
  SELECT
    base.* EXCEPT(event_params),
    firebase_screen_class.firebase_screen_class,
    firebase_event_origin.firebase_event_origin,
    ga_session_number.ga_session_number,
    firebase_screen_id.firebase_screen_id,
    engaged_session_event.engaged_session_event,
    search_term.search_term
  FROM
    ga_session_id base

    LEFT JOIN firebase_screen_class
        ON base.user_pseudo_id = firebase_screen_class.user_pseudo_id
        AND base.event_timestamp = firebase_screen_class.event_timestamp
        AND base.ga_session_id = firebase_screen_class.ga_session_id
    LEFT JOIN firebase_event_origin
        ON base.user_pseudo_id = firebase_event_origin.user_pseudo_id
        AND base.event_timestamp = firebase_event_origin.event_timestamp
        AND base.ga_session_id = firebase_event_origin.ga_session_id
    LEFT JOIN ga_session_number
        ON base.user_pseudo_id = ga_session_number.user_pseudo_id
        AND base.event_timestamp = ga_session_number.event_timestamp
        AND base.ga_session_id = ga_session_number.ga_session_id
    LEFT JOIN firebase_screen_id
        ON base.user_pseudo_id = firebase_screen_id.user_pseudo_id
        AND base.event_timestamp = firebase_screen_id.event_timestamp
        AND base.ga_session_id = firebase_screen_id.ga_session_id
    LEFT JOIN engaged_session_event
        ON base.user_pseudo_id = engaged_session_event.user_pseudo_id
        AND base.event_timestamp = engaged_session_event.event_timestamp
        AND base.ga_session_id = engaged_session_event.ga_session_id
    LEFT JOIN search_term
        ON base.user_pseudo_id = search_term.user_pseudo_id
        AND base.event_timestamp = search_term.event_timestamp
        AND base.ga_session_id = search_term.ga_session_id
)


,final AS (
  SELECT
    event_params.* EXCEPT(
      user_properties,
      user_ltv,
      event_dimensions,
      ecommerce,
      items,
      privacy_info,
      event_value_in_usd
    ) REPLACE(
      PARSE_DATE("%Y%m%d", event_date) AS event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
      TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp
    )
  FROM event_params
  WHERE search_term IS NOT NULL OR search_term != ''
)

SELECT
  *
FROM final

