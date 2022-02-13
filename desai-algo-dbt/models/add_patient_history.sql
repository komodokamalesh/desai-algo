{{
  config(
    materialized='table', 
    transient=true
  	)
}}


--create time period flags and code arrays

with TIME_PERIOD_AND_CODE_ARRAYS as (select 
a.upk_key2,
hf_incident_date,
e.claim_date,
datediff('day', e.claim_date, hf_incident_date) as days_from_incident,
(days_from_incident between -30 and 30) as on_index_date,
(days_from_incident between -30 and 210) as prior_to_index_date,
(days_from_incident between -60 and 210) as prior_to_index_date_plus_30,
diagnosis_array as dx_array,
procedure_array as px_array,
array_cat(ifnull(ndc_array, array_construct()), iff(rx.ndc is null,array_construct(),
  array_construct(rx.ndc))) as ndc_array
from {{ref('add_age_gender_hf_incident_date')}} a
left join {{source('encounters', 'mx_lite')}} e
on a.upk_key2=e.upk_key2
left join {{source('encounters', 'rx')}} rx
on a.upk_key2=rx.upk_key2
where (e.claim_date between '2018-06-01' and '2020-07-31')
and (rx.claim_date between '2018-06-01' and '2020-07-31')
and (on_index_date or prior_to_index_date or prior_to_index_date_plus_30)),


--create table of aggregated dx arrays and time period 1
DX_ARRAY_TABLE1 as (SELECT upk_key2,
       array_agg (distinct F.value) AS dx_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.dx_array)) F
       where on_index_date
       group by upk_key2),


--create table of aggregated dx arrays and time period 2
DX_ARRAY_TABLE2 as (SELECT upk_key2,
       array_agg (distinct F.value) AS dx_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.dx_array)) F
       where prior_to_index_date
       group by upk_key2),

--create table of aggregated dx arrays and time period 3
DX_ARRAY_TABLE3 as (SELECT upk_key2,
       array_agg (distinct F.value) AS dx_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.dx_array)) F
       where prior_to_index_date_plus_30
       group by upk_key2),

--create table of aggregated px arrays and time period 1
PX_ARRAY_TABLE1 as (SELECT upk_key2,
       array_agg (distinct F.value) AS px_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.px_array)) F
       where on_index_date
       group by upk_key2),

--create table of aggregated dx arrays and time period 2
PX_ARRAY_TABLE2 as (SELECT upk_key2,
       array_agg (distinct F.value) AS px_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.px_array)) F
       where prior_to_index_date
       group by upk_key2),

--create table of aggregated dx arrays and time period 3
PX_ARRAY_TABLE3 as (SELECT upk_key2,
       array_agg (distinct F.value) AS px_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.px_array)) F
       where prior_to_index_date_plus_30
       group by upk_key2),

--create table of aggregated px arrays and time period 1
NDC_ARRAY_TABLE1 as (SELECT upk_key2,
       array_agg (distinct F.value) AS ndc_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.ndc_array)) F
       where on_index_date
       group by upk_key2),

--create table of aggregated dx arrays and time period 2
NDC_ARRAY_TABLE2 as (SELECT upk_key2,
       array_agg (distinct F.value) AS ndc_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.ndc_array)) F
       where prior_to_index_date
       group by upk_key2),

--create table of aggregated dx arrays and time period 3
NDC_ARRAY_TABLE3 as (SELECT upk_key2,
       array_agg (distinct F.value) AS ndc_array
        FROM   TIME_PERIOD_AND_CODE_ARRAYS, 
       Table(Flatten(TIME_PERIOD_AND_CODE_ARRAYS.ndc_array)) F
       where prior_to_index_date_plus_30
       group by upk_key2)

select a.upk_key2,
a.patient_gender,
a.hf_hospital_count,
a.hf_incident_soc,
a.age,
dx1.dx_array as dx_on_index_date,
dx2.dx_array as dx_prior_to_index_date,
dx3.dx_array as dx_prior_to_index_date_plus_30,
px1.px_array as px_on_index_date,
px2.px_array as px_prior_to_index_date,
px3.px_array as px_prior_to_index_date_plus_30,
ndc1.ndc_array as ndc_on_index_date,
ndc2.ndc_array as ndc_prior_to_index_date,
ndc3.ndc_array as ndc_prior_to_index_date_plus_30
from {{ref('add_age_gender_hf_incident_date')}} a
left join DX_ARRAY_TABLE1 as dx1
on dx1.upk_key2=a.upk_key2
left join DX_ARRAY_TABLE2 as dx2
on dx2.upk_key2=a.upk_key2
left join DX_ARRAY_TABLE3 as dx3
on dx3.upk_key2=a.upk_key2
left join PX_ARRAY_TABLE1 as px1
on px1.upk_key2=a.upk_key2
left join PX_ARRAY_TABLE2 as px2
on px2.upk_key2=a.upk_key2
left join PX_ARRAY_TABLE3 as px3
on px3.upk_key2=a.upk_key2
left join NDC_ARRAY_TABLE1 as ndc1
on ndc1.upk_key2=a.upk_key2
left join NDC_ARRAY_TABLE2 as ndc2
on ndc2.upk_key2=a.upk_key2
left join NDC_ARRAY_TABLE3 as ndc3
on ndc3.upk_key2=a.upk_key2






