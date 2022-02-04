{{
  config(
    materialized='table'
  	)
}}


select distinct
upk_key2,
iff(patient_gender='M', 1, 0)::float as gender,
age::float as age
from {{ref('add_age_gender_hf_incident_date')}}
where hf_incident_date is not null
