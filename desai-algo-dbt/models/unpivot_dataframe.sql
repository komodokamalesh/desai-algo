{{
  config(
    materialized='table'
  	)
}}


select * from {{ref('convert_to_dataframe')}}
    unpivot(x for col_name in (gender, age))
    order by upk_key2