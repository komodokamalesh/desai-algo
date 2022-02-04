{{
  config(
    materialized='table'
  	)
}}


select 
distinct upk_key2  from
{{source('unspecified_patients', 'patients')}} 
sample (10 rows)