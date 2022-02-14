{{
  config(
    materialized='table'
  	)
}}

--map drug names to columns in data matrix

SELECT
array_construct_compact(ingredient1, ingredient2, 
  ingredient3, ingredient3) as ingredient_array,
iff(arrays_overlap(ingredient_array, array_construct('benazepril', 'captopril', 'enalapril',
'fosinopril', 'lisinopril', 'moexipril','perindopril', 'quinapril', 'ramipril',
'trandolapril')),'RX_ACE', null ) as RX_ACE,
iff(arrays_overlap(ingredient_array, array_construct('eplerenone, spironolactone')),
  'RX_ANTANGONIST', null ) as RX_ANTAGONIST,
iff(arrays_overlap(ingredient_array, array_construct('acebutolol', 'atenolol', 'betaxolol',
'bisoprolol', 'carteolol', 'carvedilol', 'esmolol',
'labetalol', 'metoprolol', 'nadolol', 'nebivolol',
'penbutolol', 'pindolol', 'propranolol',
'timolol')),
  'RX_BBLOCKER', null) as RX_BBLOCKER, 
iff(arrays_overlap(ingredient_array, array_construct('digoxin')),
  'RX_DIGOXIN', null ) as RX_DIGOXIN, 
iff(arrays_overlap(ingredient_array, array_construct('bumetanide', 'furosemide',
 'torsemide','ethacrynic')),
  'RX_LOOP_DIURETIC', null ) as RX_LOOP_DIURETIC, 
iff(arrays_overlap(ingredient_array, array_construct('nitroglycerin', 'isosorbide dinitrate',
'isosorbide mononitrate', 'ranolazine')),
  'RX_NITRATES', null ) as RX_NITRATES,  
iff(arrays_overlap(ingredient_array, array_construct('bendroflumethiazide', 'benzthiazide',
'chlorothiazide', 'chlorthalidone',
'hydrochlorothiazide', 'indapamide',
'methyclothiazide', 'metolazone',
'polythiazide', 'quinethazone',
'trichlormethiazide')),
  'RX_THIAZIDE', null ) as RX_THIAZIDE,
iff(arrays_overlap(ingredient_array, array_construct('orlistat', 'sibutramine',
'phentermine', 'benzphetamine','phendimetrazine', 'diethylpropion')), 
'RX_OBESITY',null) as RX_OBESITY,
coalesce (RX_ACE, RX_ANTAGONIST, RX_BBLOCKER, RX_DIGOXIN, RX_LOOP_DIURETIC, RX_NITRATES,
RX_THIAZIDE, RX_OBESITY) as COLUMN_NAME    
from {{ref('drug_ingredients_table')}}