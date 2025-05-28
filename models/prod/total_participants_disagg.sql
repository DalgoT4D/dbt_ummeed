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
        COALESCE(reg_attending_program, 'Not Available') AS participant_category,
        pid,
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
        participant_category,
        -- Generate a unique `pid` for each missing participant, ensuring it's a string
        CONCAT('nr_', LPAD((ROW_NUMBER() OVER ())::TEXT, 6, '0')) AS pid,
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
