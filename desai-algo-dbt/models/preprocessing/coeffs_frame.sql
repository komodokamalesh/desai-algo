{{
  config(
    materialized='table'
  	)
}}

--pull in coefficients seed file
select 
* from
{{ref('model_coef_csv')}} 