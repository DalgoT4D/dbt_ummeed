{{ config(materialized='table') }}

WITH appointment_data AS(
    SELECT DISTINCT
    NULLIF(age, '')::VARCHAR AS patient_age,
    NULLIF(city	, '') AS city,
    NULLIF(mrno, '')::VARCHAR AS mrno,
    NULLIF(dob, '') AS dob,
    NULLIF(gender, '') AS patient_gender,
    NULLIF(unit, '') AS unit,
    NULLIF(roomno, '') AS room_no,
    NULLIF(address, '') AS address,
    eventid AS event_id,
    NULLIF(mobileno, '')::VARCHAR AS mobile_no,
    NULLIF(createdby, '') AS created_by,
    --NULLIF(consultant, '')::VARCHAR AS doctor,
    -- Cleaned doctor name value by removing titles, salutations and extra spaces
    -- The regex removes common titles like Dr., Miss, Ms., Mr., Mrs., and Mister
    NULLIF(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                TRIM(
                    REGEXP_REPLACE(COALESCE(consultant, ''), '(?i)(^|\s)(dr\.?|miss|ms\.?|mr\.?|mister|mrs\.?)(\s|$)', ' ')
                ),
                '\s+', ' '
            ),
            '^\s+|\s+$', ''
        ), ''
    )::VARCHAR AS doctor,
    NULLIF(department, '')::VARCHAR AS department,
    NULLIF(createddate, '')::DATE AS created_date,
    NULLIF(eventstatus, '') AS event_status,
    isprocessed::VARCHAR,
    NULLIF(patientname, '') AS patient_name,
    NULLIF(slotendtime, '') AS slot_end_time,
    NULLIF(updateddate, '') AS updated_date,

    --Cancellation Logic

    -- Extracting the cancel code (First two parts: 'XX - YY')
    CASE 
        WHEN cancelreason LIKE 'PC%' THEN TRIM(SPLIT_PART(cancelreason, '.', 1))
        WHEN cancelreason LIKE 'UC%' THEN TRIM(SPLIT_PART(cancelreason, '.', 1))
        WHEN cancelreason LIKE 'OC%' THEN TRIM(SPLIT_PART(cancelreason, '.', 1))
        WHEN cancelreason LIKE 'XC%' THEN TRIM(SPLIT_PART(cancelreason, '.', 1))
        WHEN cancelreason LIKE 'NS%' THEN TRIM(SPLIT_PART(cancelreason, '.', 1))
        WHEN cancelreason = 'Patient Cancelled 48 hours before appointment' THEN 'PC' 
    END AS cancel_code,

    -- Identifying who cancelled the appointment (NULL if cancelreason is empty)
    CASE 
        WHEN cancelreason IS NULL OR TRIM(cancelreason) = '' THEN NULL
        WHEN cancelreason LIKE 'UC%' THEN 'Ummeed'
        WHEN cancelreason LIKE 'PC%' OR cancelreason = 'Patient Cancelled 48 hours before appointment' THEN 'Patient'
        ELSE 'Other'
    END AS cancelled_by,

    -- Extracting the actual cancellation reason without numbers
    CASE 
        WHEN cancelreason = 'Patient Cancelled 48 hours before appointment' THEN cancelreason
        WHEN cancelreason IS NULL OR TRIM(cancelreason) = '' THEN NULL
        ELSE TRIM(REGEXP_REPLACE(SPLIT_PART(cancelreason, '- ', 2), '^\d+\.\s*', ''))
    END AS cancellation_reason,

    NULLIF(slotstarttime, '') AS slot_start_time,
    TO_TIMESTAMP(NULLIF(eventvalidfrom, ''), 'Mon DD, YYYY HH12:MI:SS PM') AS event_valid_from,
    TO_TIMESTAMP(NULLIF(eventvalidto, ''), 'Mon DD, YYYY HH12:MI:SS PM') AS event_valid_to

FROM {{ source('source_ummeed_ict_health', 'appointment_details') }} AS appointment_data
),

promotions as (
    select
        doctor_name,
        doctor_lvl,
        promotion_date,
        lead(promotion_date) over (
            partition by doctor_name 
            order by promotion_date
        ) as next_promotion_date
    from {{ source('source_ummeed_ict_health', 'dim_doctor_level_mapping')}}    
)

SELECT 
    ad.*,
    COALESCE(p.doctor_lvl, 'Not Available') AS doctor_level  -- Mapped from dim_doctor_level_mapping
FROM appointment_data AS ad
LEFT JOIN promotions AS p
        ON ad.doctor = p.doctor_name 
        AND ad.created_date >= p.promotion_date
        AND (
          p.next_promotion_date is NULL 
          OR ad.created_date < p.next_promotion_date
     )
