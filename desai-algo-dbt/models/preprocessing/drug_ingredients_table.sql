{{
  config(
    materialized='table'
  	)
}}


--pull in drug incredients files

SELECT *  
from {{ref('drug_ingredients')}}