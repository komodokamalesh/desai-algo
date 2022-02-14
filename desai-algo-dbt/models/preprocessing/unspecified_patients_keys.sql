{{
  config(
    materialized='table'
  	)
}}

--pull in patient keys (provided by Analytics Consulting/Nikos Zarikos)

select 
distinct upk_key2  from
{{source('unspecified_patients', 'patients')}} 
