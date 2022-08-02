SELECT
  attendance_id,
  attendances.attendances_status AS attendances_attendances_status,
  attendances.attendances_date AS attendances_attendances_date,
  created_at,
  modified_at,
  published_timestamp
FROM
  logee-data-prod.L2_visibility.lgd_attendance
WHERE
  modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}',
  UNNEST(attendances) AS attendances