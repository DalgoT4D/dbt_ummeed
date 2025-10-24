{{ config(
    materialized="table"
) }}

WITH participant_impact_clean AS (
    SELECT
        EXTRACT(YEAR FROM start_date) AS year,
        department,
        state_name,
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        CASE
         
            WHEN LOWER(reg_attending_program) LIKE '%parent%' 
                OR LOWER(reg_attending_program) LIKE '%grandparent%' 
                OR LOWER(reg_attending_program) LIKE '%sibling%' 
                OR LOWER(reg_attending_program) LIKE '%caregiver%' 
                THEN 'Parent/ Grandparent/ Sibling/ Caregiver'
            WHEN LOWER(reg_attending_program) LIKE '%community worker%' 
                OR LOWER(reg_attending_program) LIKE '%social worker%' 
                THEN 'Community Worker/ Social Worker'
            WHEN LOWER(reg_attending_program) LIKE '%educator%' 
                OR LOWER(reg_attending_program) LIKE '%teacher%' 
                THEN 'Educator'
            WHEN LOWER(reg_attending_program) LIKE '%student%' 
                THEN 'Student'
            WHEN LOWER(reg_attending_program) LIKE '%professional%' 
                THEN 'Professional'
            WHEN reg_attending_program IS NULL OR TRIM(reg_attending_program) = '' 
                THEN 'Not Available'
            ELSE 'Other'
        END AS participant_category,  
        pid,
        CASE
            WHEN participant_gender IS NULL OR TRIM(participant_gender) = '' THEN 'Not Available'
            WHEN LOWER(TRIM(participant_gender)) ~ '^(m|male|man|boy|cis[- ]?male)$' THEN 'Male'
            WHEN LOWER(TRIM(participant_gender)) ~ '^(f|female|woman|girl|cis[- ]?female)$' THEN 'Female'
            WHEN LOWER(TRIM(participant_gender)) ~ '(prefer not|prefer not to|prefer not to say|prefer not to disclose|do not wish|dont want|decline|no response|not disclose|rather not say)' THEN 'Prefer Not To Mention'
            ELSE 'Other'
        END AS participant_gender,
        training_indirect_reach_monthly,
        standardized_org_name,
        'participant_impact' AS source,

        -- Calculate Financial Year
        CASE 
            WHEN
                EXTRACT(MONTH FROM start_date) >= 4 
                THEN CONCAT(
                    EXTRACT(YEAR FROM start_date), 
                    '-', 
                    EXTRACT(YEAR FROM start_date) + 1
                )
            ELSE CONCAT(
                EXTRACT(YEAR FROM start_date) - 1, 
                '-', 
                EXTRACT(YEAR FROM start_date)
            )
        END AS financial_year,

        --Month
        TO_CHAR(start_date, 'Month') AS month,

        -- Quarter
        CASE 
            WHEN EXTRACT(MONTH FROM start_date) BETWEEN 1 AND 3 THEN 'Q4'
            WHEN EXTRACT(MONTH FROM start_date) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN EXTRACT(MONTH FROM start_date) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN EXTRACT(MONTH FROM start_date) BETWEEN 10 AND 12 THEN 'Q3'
        END AS quarter

    FROM {{ ref('participant_impact') }}
),

no_registrations_expanded AS (
    SELECT
        EXTRACT(YEAR FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) AS year,
        department,
        NULL::TEXT AS state_name,
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        CASE 
            WHEN LOWER(participant_category) LIKE '%parent%' 
            OR LOWER(participant_category) LIKE '%grandparent%' 
            OR LOWER(participant_category) LIKE '%sibling%' 
            OR LOWER(participant_category) LIKE '%caregiver%' 
            THEN 'Parent/ Grandparent/ Sibling/ Caregiver'
            WHEN LOWER(participant_category) LIKE '%community worker%' 
            OR LOWER(participant_category) LIKE '%social worker%' 
            THEN 'Community Worker/ Social Worker'
            WHEN LOWER(participant_category) LIKE '%educator%' 
            OR LOWER(participant_category) LIKE '%teacher%' 
            THEN 'Educator'
            WHEN LOWER(participant_category) LIKE '%student%' 
            THEN 'Student'
            WHEN LOWER(participant_category) LIKE '%professional%' 
            THEN 'Professional'
            WHEN participant_category IS NULL OR TRIM(participant_category) = '' 
            THEN 'Not Available'
            ELSE 'Other'
        END AS participant_category,
        -- Generate a unique `pid` for each missing participant, ensuring it's a string
        CONCAT('nr_', LPAD((ROW_NUMBER() OVER ())::TEXT, 6, '0')) AS pid,
        CASE
            WHEN participant_gender IS NULL OR TRIM(participant_gender) = '' THEN 'Not Available'
            WHEN LOWER(TRIM(participant_gender)) ~ '^(m|male|man|boy|cis[- ]?male)$' THEN 'Male'
            WHEN LOWER(TRIM(participant_gender)) ~ '^(f|female|woman|girl|cis[- ]?female)$' THEN 'Female'
            WHEN LOWER(TRIM(participant_gender)) ~ '(prefer not|prefer not to|prefer not to say|prefer not to disclose|do not wish|dont want|decline|no response|not disclose|rather not say)' THEN 'Prefer Not To Mention'
            ELSE 'Other'
        END AS participant_gender,
        0 AS training_indirect_reach_monthly,
        NULL::TEXT AS standardized_org_name,
        'no_registrations' AS source,
        -- Calculate Financial Year
        CASE 
            WHEN EXTRACT(MONTH FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) >= 4 
                THEN CONCAT(
                    EXTRACT(YEAR FROM TO_DATE(start_date_str, 'DD/MM/YYYY')), '-', 
                    EXTRACT(YEAR FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) + 1
                )
            ELSE CONCAT(
                EXTRACT(YEAR FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) - 1, '-', 
                EXTRACT(YEAR FROM TO_DATE(start_date_str, 'DD/MM/YYYY'))
            )
        END AS financial_year,

        -- Month
        TO_CHAR(TO_DATE(start_date_str, 'DD/MM/YYYY'), 'Month') AS month,

        -- Quarter
        CASE 
            WHEN EXTRACT(MONTH FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) BETWEEN 1 AND 3 
                THEN 'Q4'
            WHEN EXTRACT(MONTH FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) BETWEEN 4 AND 6 
                THEN 'Q1'
            WHEN EXTRACT(MONTH FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) BETWEEN 7 AND 9 
                THEN 'Q2'
            WHEN EXTRACT(MONTH FROM TO_DATE(start_date_str, 'DD/MM/YYYY')) BETWEEN 10 AND 12 
                THEN 'Q3'
        END AS quarter

    FROM {{ ref('no_registration') }}
    CROSS JOIN GENERATE_SERIES(1, participant_count)  
)

SELECT * FROM participant_impact_clean
UNION ALL
SELECT * FROM no_registrations_expanded
