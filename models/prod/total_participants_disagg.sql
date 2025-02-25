{{ config(
    materialized="table"
) }}

WITH participant_impact_clean AS (
    -- ✅ Keep participant_impact as it is, no need to deduplicate
    SELECT
        EXTRACT(YEAR FROM COALESCE(
            TO_DATE(updated_on, 'DD/MM/YYYY'), 
            TO_DATE(created_on, 'DD/MM/YYYY')
        )) AS year,
        state_name,
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        reg_attending_program AS participant_category,
        pid,
        'participant_impact' AS source
    FROM {{ ref('participant_impact') }}
),

no_registrations_expanded AS (
    -- ✅ Convert aggregated counts into individual rows with generated `pid`
    SELECT
        NULL::INTEGER AS year,  -- No year info in `no_registrations`
        NULL::TEXT AS state_name,
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        participant_category,
        -- Generate a unique `pid` for each missing participant, ensuring it's a string
        CONCAT('nr_', LPAD(CAST(ROW_NUMBER() OVER () AS TEXT), 6, '0')) AS pid,
        'no_registrations' AS source
    FROM {{ ref('int_no_registration') }}
    CROSS JOIN generate_series(1, participant_count)  -- ✅ Expands rows based on count
)

-- ✅ Simply UNION both datasets, keeping them identical
SELECT * FROM participant_impact_clean
UNION ALL
SELECT * FROM no_registrations_expanded