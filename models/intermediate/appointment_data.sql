{{ config(materialized='table') }}

SELECT
NULLIF(age, '') AS age,
NULLIF(city	, '') AS city,
NULLIF(mrno, '') AS mrno,
NULLIF(dob, '') AS dob,
NULLIF(gender, '') AS gender,
NULLIF(unit, '') AS unit,
NULLIF(roomno, '') AS room_no,
NULLIF(address, '') AS address,
eventid AS event_id,
NULLIF(mobileno, '') AS mobile_no,
NULLIF(createdby, '') AS created_by,
NULLIF(consultant, '') AS consultant,
NULLIF(department, '') AS department,
NULLIF(createddate, '') AS created_date,
NULLIF(eventstatus, '') AS event_status,
isprocessed,
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
        ELSE NULL 
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
TO_TIMESTAMP(eventvalidfrom, 'Mon DD, YYYY HH12:MI:SS PM') AS event_valid_from,
TO_TIMESTAMP(eventvalidto, 'Mon DD, YYYY HH12:MI:SS PM') AS event_valid_to

FROM {{ source('source_ummeed_ict_health', 'appointment_details') }}