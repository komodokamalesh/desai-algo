{{
  config(
    materialized='table'
  	)
}}


select 
* from
{{ref('coefs_csv')}} 