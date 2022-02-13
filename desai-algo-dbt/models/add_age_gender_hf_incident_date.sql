{{
  config(
    materialized='table', 
    transient=true
  	)
}}


--flag HF incdents over time period and list of patients

with HF_INCIDENT_FLAG as (select
u.upk_key2,
e.claim_date,
e.visit_id,
e.encounter_key,
array_compact(array_construct(DA,D1,D2,D3,D4,D5,D6,D7,D8,D9,D10,D11,D12,D13,D14,D15,D16,D17,D18,D19,
D20,D21,D22,D23,D24,D25,D26,diagnosis_e_code_1,diagnosis_e_code_2)) as dx_array,
arrays_overlap(dx_array, (select HF from {{ref('claims_codes_table')}})) as hf_incident
from 
{{ref('unspecified_patients_keys')}} u
join {{source('encounters', 'mx')}} e
on u.upk_key2=e.upk_key2
where e.claim_date between '2018-07-01' and '2020-06-30'),

--calculate date of HF as last date in range


HF_INCIDENT_CLAIM as (select
upk_key2,
claim_date as hf_incident_date,
encounter_key,
visit_id
FROM (SELECT *, ROW_NUMBER () OVER(PARTITION BY upk_key2 ORDER BY claim_date ASC) AS rn
FROM HF_INCIDENT_FLAG
) WHERE rn = 1),


--count hospital visits
HF_HOSPITAL_COUNT as (select 
hff.upk_key2,
count(distinct hff.visit_id) as hf_hospital_count
from HF_INCIDENT_FLAG hff
join HF_INCIDENT_CLAIM as hfi
on hff.upk_key2=hfi.upk_key2
left join {{source('encounters', 'visits')}} v
on hfi.visit_id=v.visit_id
where datediff('day', hff.claim_date, hf_incident_date) between 0 and 60
and hff.hf_incident and v.visit_setting_of_care='Inpatient Visit'
group by hff.upk_key2)


--merge HF incident date with gender and age, HF hospital visits
select 
u.upk_key2,
p.patient_gender,
hf_incident_date,
ifnull(hc.hf_hospital_count,0) as hf_hospital_count,
v.visit_setting_of_care as hf_incident_soc,
datediff('year', p.patient_dob, hf_incident_date) as age
from {{ref('unspecified_patients_keys')}} u
join {{source('patients', 'patients_summary')}} p
on p.upk_key2=u.upk_key2
left join HF_INCIDENT_CLAIM as hfi
on u.upk_key2=hfi.upk_key2
left join {{source('encounters', 'visits')}} v
on hfi.visit_id=v.visit_id
left join HF_HOSPITAL_COUNT hc
on u.upk_key2=hc.upk_key2

