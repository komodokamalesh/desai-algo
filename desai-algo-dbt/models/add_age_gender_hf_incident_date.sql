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
array_compact(array_construct(DA,D1,D2,D3,D4,D5,D6,D7,D8,D9,D10,D11,D12,D13,D14,D15,D16,D17,D18,D19,
D20,D21,D22,D23,D24,D25,D26,diagnosis_e_code_1,diagnosis_e_code_2)) as dx_array,
arrays_overlap(dx_array, array_construct('I0981','I110','I130',
'I132',
'I509',
'I501',
'I5020',
'I5021',
'I5022',
'I5023',
'I5030',
'I5031',
'I5032',
'I5033',
'I5040',
'I5041',
'I5042',
'I5043',
'I509')) as hf_incident
from 
{{ref('unspecified_patients_keys')}} u
join {{source('encounters', 'mx')}} e
on u.upk_key2=e.upk_key2
where e.claim_date between '2018-07-01' and '2020-06-30'),

--calculate date of HF as last date in range


HF_INCIDENT_DATE as (select
upk_key2, 
max(claim_date) as HF_INCIDENT_DATE
from HF_INCIDENT_FLAG
where HF_INCIDENT
group by upk_key2)

--merge HF incident date with gender and age
select 
u.upk_key2,
p.patient_gender,
hf_incident_date,
datediff('year', p.patient_dob, HF_INCIDENT_DATE) as age
from {{ref('unspecified_patients_keys')}} u
join {{source('patients', 'patients_summary')}} p
on p.upk_key2=u.upk_key2
left join HF_INCIDENT_DATE as id
on u.upk_key2=id.upk_key2

