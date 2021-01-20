{%- set low_watermark =  execution_date.subtract(hours=1) -%}
SELECT * FROM some_schema.table_1
WHERE event_timestamp AT TIME ZONE 'UTC' >= '{{ low_watermark }}' AT TIME ZONE 'UTC'
AND event_timestamp AT TIME ZONE 'UTC' < '{{ execution_date }}'
