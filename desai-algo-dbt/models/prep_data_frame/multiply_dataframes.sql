{{
  config(
    materialized='table'
  	)
}}

--multiply dataframes 
SELECT upk_key2, SUM(A.X * B.COEF) AS value
FROM {{ref('unpivot_dataframe')}} A
INNER JOIN {{ref('coeffs_frame')}} B
ON A.col_name = B.col_name
group by upk_key2