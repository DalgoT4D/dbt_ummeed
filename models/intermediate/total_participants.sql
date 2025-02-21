{{ config(
    materialized="table"
) }}

WITH participant_impact_agg AS (
    SELECT
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        reg_attending_program AS participant_category,
        COUNT(*) AS total_participants,
        'participant_impact' AS source
    FROM {{ ref('participant_impact') }}
    GROUP BY 1, 2, 3, 4, 5
),

no_registrations_agg AS (
    SELECT
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        participant_category,
        SUM(participant_count) AS total_participants,
        'no_registrations' AS source
    FROM {{ ref('int_no_registration') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM participant_impact_agg
UNION ALL
SELECT * FROM no_registrations_agg