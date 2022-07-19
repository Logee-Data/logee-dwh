WITH base AS (
  SELECT
    *
  FROM
    `logee-data-prod.analytics_236272400.events_{{ ds_nodash }}`
  WHERE
    event_name = 'screen_view'
)

-- EXTRACT USER_PROPERTIES
, user_properties_unnested AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    user_id,
    user_properties_unnested.key,
    COALESCE(
      user_properties_unnested.value.string_value,
      CAST(user_properties_unnested.value.int_value AS STRING),
      CAST(user_properties_unnested.value.float_value AS STRING),
      CAST(user_properties_unnested.value.double_value AS STRING)
    ) value
  FROM
    base,
    UNNEST(user_properties) user_properties_unnested
  WHERE
    user_properties_unnested.key NOT IN (
      'user_id', 'logee_user_id'
    )
)

,ga_session_id AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    user_id,
    value AS ga_session_id
  FROM
    user_properties_unnested
  WHERE
    key = 'ga_session_id'
)

,first_open_time AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    user_id,
    CAST(value AS INT64) AS first_open_time
  FROM
    user_properties_unnested
  WHERE
    key = 'first_open_time'
)

,ga_session_number AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    user_id,
    CAST(value AS INT64) AS ga_session_number
  FROM
    user_properties_unnested
  WHERE
    key = 'ga_session_number'
)

,last_advertising_id_reset AS (
  SELECT
    event_timestamp,
    user_pseudo_id,
    user_id,
    value AS last_advertising_id_reset
  FROM
    user_properties_unnested
  WHERE
    key = 'last_advertising_id_reset'
)

,base_with_user_properties AS (
    SELECT
      base.* EXCEPT(user_properties, ecommerce, items),
      ga_session_id.ga_session_id,
      first_open_time.first_open_time,
      ga_session_number.ga_session_number,
      last_advertising_id_reset.last_advertising_id_reset
    FROM
      base

      LEFT JOIN ga_session_id
      ON base.user_id = ga_session_id.user_id
      AND base.user_pseudo_id = ga_session_id.user_pseudo_id
      AND base.event_timestamp = ga_session_id.event_timestamp

      LEFT JOIN first_open_time
      ON base.user_id = first_open_time.user_id
      AND base.user_pseudo_id = first_open_time.user_pseudo_id
      AND base.event_timestamp = first_open_time.event_timestamp

      LEFT JOIN ga_session_number
      ON base.user_id = ga_session_number.user_id
      AND base.user_pseudo_id = ga_session_number.user_pseudo_id
      AND base.event_timestamp = ga_session_number.event_timestamp

      LEFT JOIN last_advertising_id_reset
      ON base.user_id = last_advertising_id_reset.user_id
      AND base.user_pseudo_id = last_advertising_id_reset.user_pseudo_id
      AND base.event_timestamp = last_advertising_id_reset.event_timestamp
)

-- EXTRACT EVENT_PARAMS
, event_params_unnested AS (
  SELECT
    event_timestamp,
    user_id,
    user_pseudo_id,
    event_params_unnested.key,
    COALESCE(
      event_params_unnested.value.string_value,
      CAST(event_params_unnested.value.int_value AS STRING),
      CAST(event_params_unnested.value.float_value AS STRING),
      CAST(event_params_unnested.value.double_value AS STRING)
    ) AS value
  FROM
    base_with_user_properties,
    UNNEST(event_params) event_params_unnested
)

,engaged_session_event AS (
  SELECT
    * EXCEPT(key, value),
    CAST(value AS INT64) AS engaged_session_event
  FROM event_params_unnested
  WHERE key = 'engaged_session_event'
)

,ga_session_number_event AS (
  SELECT
    * EXCEPT(key, value),
    CAST(value AS INT64) AS ga_session_number
  FROM event_params_unnested
  WHERE key = 'ga_session_number'
)

,ga_session_id_event AS (
  SELECT
    * EXCEPT(key, value),
    value AS ga_session_id
  FROM event_params_unnested
  WHERE key = 'ga_session_id'
)

,firebase_event_origin AS (
  SELECT
    * EXCEPT(key, value),
    value AS firebase_event_origin
  FROM event_params_unnested
  WHERE key = 'firebase_event_origin'
)

,session_engaged AS (
  SELECT
    * EXCEPT(key, value),
    CAST(value AS INT64) AS session_engaged
  FROM event_params_unnested
  WHERE key = 'session_engaged'
)

,firebase_screen_id AS (
  SELECT
    * EXCEPT(key, value),
    value AS firebase_screen_id
  FROM event_params_unnested
  WHERE key = 'firebase_screen_id'
)

,firebase_screen_class AS (
  SELECT
    * EXCEPT(key, value),
    value AS firebase_screen_class
  FROM event_params_unnested
  WHERE key = 'firebase_screen_class'
)

,engagement_time_msec AS (
  SELECT
    * EXCEPT(key, value),
    CAST(value AS INT64) AS engagement_time_msec
  FROM event_params_unnested
  WHERE key = 'engagement_time_msec'
)

,entrances AS (
  SELECT
    * EXCEPT(key, value),
    CAST(value AS INT64) AS entrances
  FROM event_params_unnested
  WHERE key = 'entrances'
)

,firebase_previous_id AS (
  SELECT
    * EXCEPT(key, value),
    value AS firebase_previous_id
  FROM event_params_unnested
  WHERE key = 'firebase_previous_id'
)

, firebase_previous_class AS (
    SELECT
        * EXCEPT(key, value),
        value AS firebase_previous_class
    FROM event_params_unnested
    WHERE key = 'firebase_previous_class'
)

, product_id AS (
    SELECT
        * EXCEPT(key, value),
        value AS product_id
    FROM event_params_unnested
    WHERE key = 'product_id'
)

, search_term AS (
    SELECT
        * EXCEPT(key, value),
        value AS search_term
    FROM event_params_unnested
    WHERE key = 'search_term'
)

, firebase_conversion AS (
    SELECT
        * EXCEPT(key, value),
        CAST(value AS INT64) AS firebase_conversion
    FROM event_params_unnested
    WHERE key = 'firebase_conversion'
)

-- , update_with_analytics AS (
--     SELECT
--         * EXCEPT(key, value),
--         CAST(value AS INT64) AS update_with_analytics
--     FROM event_params_unnested
--     WHERE key = 'update_with_analytics'
-- )

, previous_first_open_count AS (
    SELECT
        * EXCEPT(key, value),
        CAST(value AS INT64) AS previous_first_open_count
    FROM event_params_unnested
    WHERE key = 'previous_first_open_count'
)

-- , system_app_update AS (
--     SELECT
--         * EXCEPT(key, value),
--         CAST(value AS INT64) AS system_app_update
--     FROM event_params_unnested
--     WHERE key = 'system_app_update'
-- )

-- , system_app AS (
--     SELECT
--         * EXCEPT(key, value),
--         CAST(value AS INT64) AS system_app
--     FROM event_params_unnested
--     WHERE key = 'system_app'
-- )

, medium AS (
    SELECT
        * EXCEPT(key, value),
        value AS medium
    FROM event_params_unnested
    WHERE key = 'medium'
)

, campaign_info_source AS (
    SELECT
        * EXCEPT(key, value),
        value AS campaign_info_source
    FROM event_params_unnested
    WHERE key = 'campaign_info_source'
)

, source AS (
    SELECT
        * EXCEPT(key, value),
        value AS source
    FROM event_params_unnested
    WHERE key = 'source'
)

-- , fatal AS (
--     SELECT
--         * EXCEPT(key, value),
--         CAST(value AS INT64) AS fatal
--     FROM event_params_unnested
--     WHERE key = 'fatal'
-- )

-- , timestamp AS (
--     SELECT
--         * EXCEPT(key, value),
--         CAST(value AS INT64) AS timestamp
--     FROM event_params_unnested
--     WHERE key = 'timestamp'
-- )

, base_with_user_prop_and_event_params AS (
  SELECT
    base.* EXCEPT(event_params, ga_session_number, ga_session_id),
    COALESCE(
      base.ga_session_id, ga_session_id_event.ga_session_id
    ) AS ga_session_id,
    COALESCE(
      base.ga_session_number, ga_session_number_event.ga_session_number
    ) AS ga_session_number,
    engaged_session_event.engaged_session_event,
    firebase_event_origin.firebase_event_origin,
    -- session_engaged.session_engaged,
    firebase_screen_id.firebase_screen_id,
    firebase_screen_class.firebase_screen_class,
    engagement_time_msec.engagement_time_msec,
    entrances.entrances,
    firebase_previous_id.firebase_previous_id,
    firebase_previous_class.firebase_previous_class,
    -- product_id.product_id,
    -- search_term.search_term,
    firebase_conversion.firebase_conversion,
    -- update_with_analytics.update_with_analytics,
    -- previous_first_open_count.previous_first_open_count,
    -- system_app_update.system_app_update,
    -- system_app.system_app,
    -- medium.medium,
    -- campaign_info_source.campaign_info_source,
    -- source.source,
    -- fatal.fatal,
    -- timestamp.timestamp
  FROM
    base_with_user_properties base

    LEFT JOIN engaged_session_event
        ON base.event_timestamp = engaged_session_event.event_timestamp
        AND base.user_pseudo_id = engaged_session_event.user_pseudo_id
        AND base.user_id = engaged_session_event.user_id
    LEFT JOIN ga_session_number_event
        ON base.event_timestamp = ga_session_number_event.event_timestamp
        AND base.user_pseudo_id = ga_session_number_event.user_pseudo_id
        AND base.user_id = ga_session_number_event.user_id
    LEFT JOIN ga_session_id_event
        ON base.event_timestamp = ga_session_id_event.event_timestamp
        AND base.user_pseudo_id = ga_session_id_event.user_pseudo_id
        AND base.user_id = ga_session_id_event.user_id
    LEFT JOIN firebase_event_origin
        ON base.event_timestamp = firebase_event_origin.event_timestamp
        AND base.user_pseudo_id = firebase_event_origin.user_pseudo_id
        AND base.user_id = firebase_event_origin.user_id
    -- LEFT JOIN session_engaged
    --     ON base.event_timestamp = session_engaged.event_timestamp
    --     AND base.user_pseudo_id = session_engaged.user_pseudo_id
    --     AND base.user_id = session_engaged.user_id
    LEFT JOIN firebase_screen_id
        ON base.event_timestamp = firebase_screen_id.event_timestamp
        AND base.user_pseudo_id = firebase_screen_id.user_pseudo_id
        AND base.user_id = firebase_screen_id.user_id
    LEFT JOIN firebase_screen_class
        ON base.event_timestamp = firebase_screen_class.event_timestamp
        AND base.user_pseudo_id = firebase_screen_class.user_pseudo_id
        AND base.user_id = firebase_screen_class.user_id
    LEFT JOIN engagement_time_msec
        ON base.event_timestamp = engagement_time_msec.event_timestamp
        AND base.user_pseudo_id = engagement_time_msec.user_pseudo_id
        AND base.user_id = engagement_time_msec.user_id
    LEFT JOIN entrances
        ON base.event_timestamp = entrances.event_timestamp
        AND base.user_pseudo_id = entrances.user_pseudo_id
        AND base.user_id = entrances.user_id
    LEFT JOIN firebase_previous_id
        ON base.event_timestamp = firebase_previous_id.event_timestamp
        AND base.user_pseudo_id = firebase_previous_id.user_pseudo_id
        AND base.user_id = firebase_previous_id.user_id
    LEFT JOIN firebase_previous_class
        ON base.event_timestamp = firebase_previous_class.event_timestamp
        AND base.user_pseudo_id = firebase_previous_class.user_pseudo_id
        AND base.user_id = firebase_previous_class.user_id
    -- LEFT JOIN product_id
    --     ON base.event_timestamp = product_id.event_timestamp
    --     AND base.user_pseudo_id = product_id.user_pseudo_id
    --     AND base.user_id = product_id.user_id
    -- LEFT JOIN search_term
    --     ON base.event_timestamp = search_term.event_timestamp
    --     AND base.user_pseudo_id = search_term.user_pseudo_id
    --     AND base.user_id = search_term.user_id
    LEFT JOIN firebase_conversion
        ON base.event_timestamp = firebase_conversion.event_timestamp
        AND base.user_pseudo_id = firebase_conversion.user_pseudo_id
        AND base.user_id = firebase_conversion.user_id
    -- LEFT JOIN update_with_analytics
    --     ON base.event_timestamp = update_with_analytics.event_timestamp
    --     AND base.user_pseudo_id = update_with_analytics.user_pseudo_id
    --     AND base.user_id = update_with_analytics.user_id
    -- LEFT JOIN previous_first_open_count
    --     ON base.event_timestamp = previous_first_open_count.event_timestamp
    --     AND base.user_pseudo_id = previous_first_open_count.user_pseudo_id
    --     AND base.user_id = previous_first_open_count.user_id
    -- LEFT JOIN system_app_update
    --     ON base.event_timestamp = system_app_update.event_timestamp
    --     AND base.user_pseudo_id = system_app_update.user_pseudo_id
    --     AND base.user_id = system_app_update.user_id
    -- LEFT JOIN system_app
    --     ON base.event_timestamp = system_app.event_timestamp
    --     AND base.user_pseudo_id = system_app.user_pseudo_id
    --     AND base.user_id = system_app.user_id
    -- LEFT JOIN medium
    --     ON base.event_timestamp = medium.event_timestamp
    --     AND base.user_pseudo_id = medium.user_pseudo_id
    --     AND base.user_id = medium.user_id
    -- LEFT JOIN campaign_info_source
    --     ON base.event_timestamp = campaign_info_source.event_timestamp
    --     AND base.user_pseudo_id = campaign_info_source.user_pseudo_id
    --     AND base.user_id = campaign_info_source.user_id
    -- LEFT JOIN source
    --     ON base.event_timestamp = source.event_timestamp
    --     AND base.user_pseudo_id = source.user_pseudo_id
    --     AND base.user_id = source.user_id
    -- LEFT JOIN fatal
    --     ON base.event_timestamp = fatal.event_timestamp
    --     AND base.user_pseudo_id = fatal.user_pseudo_id
    --     AND base.user_id = fatal.user_id
    -- LEFT JOIN timestamp
    --     ON base.event_timestamp = timestamp.event_timestamp
    --     AND base.user_pseudo_id = timestamp.user_pseudo_id
    --     AND base.user_id = timestamp.user_id
)

-- FINALIZING
, final AS (
  SELECT
    * EXCEPT(
      event_value_in_usd,
      event_server_timestamp_offset,
      privacy_info,
      user_ltv,
      event_dimensions,
      stream_id
    ) REPLACE (
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      TIMESTAMP_MICROS(event_previous_timestamp) AS event_previous_timestamp,
      TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
      TIMESTAMP_MILLIS(first_open_time) AS first_open_time
    )
  FROM base_with_user_prop_and_event_params
)

SELECT * FROM final
