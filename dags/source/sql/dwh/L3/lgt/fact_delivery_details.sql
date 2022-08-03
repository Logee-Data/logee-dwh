SELECT
  *
  EXCEPT (
    delivery_detail_status
  )
FROM  
  `logee-data-prod.L2_visibility.dma_logee_delivery_details`
WHERE
    modified_at BETWEEN '{{ execution_date }}' AND '{{ next_execution_date }}'