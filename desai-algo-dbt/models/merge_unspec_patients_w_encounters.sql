{{
  config(
    materialized='table', 
    transient=true
  	)
}}


--pull sample? of mx claims with service line information
--note: need to review QA

select
e.claim_date
,e.upk_key2
,e.encounter_key
,e.visit_id
from 
{{ref('unspecified_patients_keys')}} u
 join {{source('encounters', 'mx')}} e
on u.upk_key2=e.upk_key2
where e.claim_date>='2022-01-01'
