{{
  config(
    materialized='table'
  	)
}}


select 
* from
{{ref('coeffs_csv')}} 