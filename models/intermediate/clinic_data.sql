{{ config(materialized='table') }}

SELECT 
    "encounterid" AS s_no,
    "mrno" AS mrn,
    "patientname" AS patient_name,
    "age" AS age,
    "gender" AS gender,
    "mobileno" AS mobile_no,
    "department" AS department,
    "doctor" AS doctor,
    TO_DATE("consultationrequestdate", 'DD-MON-YY') AS consultation_date,
    "appointmenttype" AS consultation_type,
    "unit" AS unit,
    "visitid" AS visit_no,
    "billstatus" AS billed_status,
    "invoiceno" AS invoice_no,
    "queuegenerationtime" AS queue_generation_time,
    "queueno" AS queue_no,
    "startconsultationtime" AS start_consultation_time,
    "endconsultationtime" AS end_consultation_time,
    "sponsorname" AS sponsor_name,
    "plan" AS plan,
    "assocmpny" AS associate_company
FROM {{ source('source_ummeed_ict_health', 'clinic_bay_management_data') }}
