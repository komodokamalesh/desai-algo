{{
  config(
    materialized='table'
  	)
}}


SELECT *  
from {{ref('drug_ingredients')}}