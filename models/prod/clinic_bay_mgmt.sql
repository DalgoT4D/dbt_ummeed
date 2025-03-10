{{ config(materialized='table') }}

select * from {{ref('clinic_data')}}
WHERE 
    patient_name <> 'Dummy Ummeed'
    AND consultation_type NOT IN ('Internal Review', 'Phone/ Email Query', 'Uppa Fees')
