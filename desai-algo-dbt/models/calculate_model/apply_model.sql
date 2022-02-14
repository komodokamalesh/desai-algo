{{
  config(
    materialized='table'
  	)
}}

--apply logistic transformation and thresholds to model results

SELECT upk_key2,
value,
iff(value<0, exp(value)/(1+(exp(value))), 1/(1+(exp(-value)))) as p0,
1-p0 as p1,
p0 as phf1,
p1 as phf2,
iff(phf1 > 0.4686, 'rEF', 'pEF') as ef_category
from {{ref('multiply_dataframes')}}