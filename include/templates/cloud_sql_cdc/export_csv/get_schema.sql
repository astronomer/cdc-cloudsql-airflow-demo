{% set tables = params.tables %}
SELECT 
  table_name,
  column_name,
  data_type,
  ordinal_position
FROM information_schema.columns 
WHERE table_schema = 'some_schema' 
AND table_name in (
  {%- for name in tables -%}
    {%- if loop.first -%}
    '{{ name }}'
    {%- else -%}
    ,'{{ name }}'
    {%- endif -%}
  {%- endfor -%}
)
