{{ config(materialized='table') }}


SELECT
    p.pid,
    p.participant, 
    p.primary_contact,
    p.state_name AS state, 
    p.department,
    iso."ISO Code" AS iso_code,
    p.participant_gender,
    p.course_name,
    p.course_category,
    p.course_short_name,
    p.organisation_name,
    p.standardized_org_name,
    COALESCE(p.reg_attending_program, 'Not Available') AS reg_attending_program,
    p.working_with_people_devdelay_montly, 
    CASE
        WHEN LOWER("working_with_people_devdelay_montly") = 'none' THEN 0

        WHEN LOWER("working_with_people_devdelay_montly") LIKE 'greater than %' THEN 
            CAST(
                REGEXP_REPLACE(LOWER("working_with_people_devdelay_montly"), '^greater than\s+(\d+).*$', '\1')
                AS INTEGER
            )

        WHEN "working_with_people_devdelay_montly" LIKE '%-%' THEN ROUND(
            (
                CAST(LTRIM(SPLIT_PART(TRIM("working_with_people_devdelay_montly"), '-', 1), '0') AS INTEGER) +
                CAST(LTRIM(SPLIT_PART(TRIM("working_with_people_devdelay_montly"), '-', 2), '0') AS INTEGER)
            ) / 2.0
        )::INTEGER

        ELSE 0
    END::INTEGER AS training_indirect_reach_monthly,
    p.start_date,
    EXTRACT(YEAR FROM p.start_date) AS year,
    
    -- Calculate Financial Year
    CASE 
        WHEN EXTRACT(MONTH FROM start_date) >= 4
            THEN 
                CONCAT(EXTRACT(YEAR FROM start_date), '-', EXTRACT(YEAR FROM start_date) + 1)
        ELSE 
            CONCAT(EXTRACT(YEAR FROM start_date) - 1, '-', EXTRACT(YEAR FROM start_date))
    END AS financial_year,

    -- Month
    TO_CHAR(start_date, 'Month') AS month,

    -- Quarter
    CASE 
        WHEN EXTRACT(MONTH FROM start_date) BETWEEN 1 AND 3 
            THEN 'Q4'
        WHEN EXTRACT(MONTH FROM start_date) BETWEEN 4 AND 6 
            THEN 'Q1'
        WHEN EXTRACT(MONTH FROM start_date) BETWEEN 7 AND 9 
            THEN 'Q2'
        WHEN EXTRACT(MONTH FROM start_date) BETWEEN 10 AND 12 
            THEN 'Q3'
    END AS quarter

FROM {{ ref('participant_impact') }} AS p
LEFT JOIN 
    prod.india_states_iso AS iso
    ON 
        LOWER(p.state_name) = LOWER(iso."State")
