{{
  config(
    materialized='table'
  	)
}}


select 
* from
{{ref('test_data_matrix')}} 