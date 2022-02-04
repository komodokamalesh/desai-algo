{{
  config(
    materialized='table'
  	)
}}


select 
* from
{{source('unspecified_patients', 'patients')}} 
sample (10000 rows)