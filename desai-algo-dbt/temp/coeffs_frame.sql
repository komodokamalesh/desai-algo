{{
  config(
    materialized='table'
  	)
}}


select 
* from
{{ref('models_coef_csv')}} 