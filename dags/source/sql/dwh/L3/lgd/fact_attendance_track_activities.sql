SELECT
  attendance_id,
  track_activities.track_activities_type AS track_activities_track_activities_type,
  track_activities.track_activities_timestamp AS track_activities_track_activities_timestamp,
  created_at,
  modified_at,
  published_timestamp
FROM
  logee-data-prod.L2_visibility.lgd_attendance
WHERE
  modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}',
  UNNEST(track_activities) AS track_activities