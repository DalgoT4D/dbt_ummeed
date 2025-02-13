{{ config(materialized='table') }}

SELECT 
    id,
    age,
    NULLIF(dob, '') AS dob,
    NULLIF(mrno, '') AS mrno,
    NULLIF(gender, '') AS gender,
    NULLIF(diagnosis, '') AS diagnosis,
    NULLIF(mobileno, '') AS mobile_no,
    NULLIF(fatherphone, '') AS father_phone,
    NULLIF(motherphone, '') AS mother_phone,
    NULLIF(fatherage, '') AS father_age,
    NULLIF(plan_name, '') AS plan_name,
    NULLIF(fathercity, '') AS father_city,
    NULLIF(fatherdist, '') AS father_dist,
    NULLIF(mothercity, '') AS mother_city,
    NULLIF(motherdist, '') AS mother_dist,
    NULLIF(pat_idn_no, '') AS pat_idn_no,
    NULLIF(fatherstate, '') AS father_state,
    NULLIF(guardianage, '') AS guardian_age,
    NULLIF(guardianpin, '') AS guardian_pin,
    isprocessed AS is_processed,
    NULLIF(motherstate, '') AS mother_state,
    NULLIF(updateddate, '') AS updated_date,
    NULLIF(guardiancity, '') AS guardian_city,
    NULLIF(guardiandist, '') AS guardian_dist,
    NULLIF(inserteddate, '') AS inserted_date,
    NULLIF(fathercountry, '') AS father_country,
    NULLIF(identity_type, '') AS identity_type,
    NULLIF(mothercountry, '') AS mother_country,
    NULLIF(guardianrstate, '') AS guardian_rstate,
    NULLIF(patient_income, '') AS patient_income,
    NULLIF(registrationtype, '') AS registration_type,
    NULLIF(service_center_name, '') AS service_center_name,

    -- Airbyte metadata columns remain unchanged
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta

FROM {{ source('source_ummeed_ict_health', 'registered_patient') }}