{{
  config(
    materialized='table'
  	)
}}

--merge model results back into list of patients

select distinct
aa.upk_key2,
ef_category
from {{ref('add_age_gender_hf_incident_date')}} aa
left join {{ref('apply_model')}} am
on am.upk_key2=aa.upk_key2