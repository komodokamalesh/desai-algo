{{
  config(
    materialized='table'
  	)
}}


select 
* from
{{ref('model_coef_csv')}} 