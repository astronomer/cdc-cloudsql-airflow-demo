{%- set low_watermark =  execution_date -%}
{%- set high_watermark =  execution_date.add(hours=1) -%}
SELECT * FROM some_schema.table_2
WHERE event_timestamp AT TIME ZONE 'UTC' >= '{{ low_watermark }}' AT TIME ZONE 'UTC'
AND event_timestamp AT TIME ZONE 'UTC' < '{{ high_watermark }}'
