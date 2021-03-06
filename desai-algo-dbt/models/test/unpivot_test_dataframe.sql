{{
  config(
    materialized='table'
  	)
}}


select * from {{ref('test_data_frame')}}
    unpivot(x for col_name in (CONSTANT, MALE,
      INDEX_DX_OUT,  AGE, DX_DEFIBRILLATOR,  HOSP_CHF,
      RX_ACE,  RX_ANTAGONIST, RX_BBLOCKER, RX_DIGOXIN,
      RX_LOOP_DIURETIC,  RX_NITRATES, RX_THIAZIDE, DX_AFIB,
      DX_ANEMIA, DX_CABG, DX_CARDIOMYOPATHY, DX_COPD, DX_DEPRESSION,
      DX_HTN_NEPHROPATHY,  DX_HYPERLIPIDEMIA, DX_HYPERTENSION, DX_HYPOTENSION,
      DX_MI, DX_OBESITY,  DX_OTH_DYSRHYTHMIA,  DX_PSYCHOSIS,  DX_RHEUMATIC_HEART,  
      DX_SLEEP_APNEA,  DX_STABLE_ANGINA,  DX_VALVE_DISORDER, HF_SYSTOLIC, 
      HF_DIASTOLIC,  HF_LEFT, HF_UNSPECIFIED))
    order by upk_key2