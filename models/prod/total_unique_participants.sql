{{ config(
    materialized="table"
) }}

WITH full_session_list AS (
    -- Collects all unique sessions
    SELECT DISTINCT
        state_name,
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        reg_attending_program
    FROM {{ ref('participant_impact') }}
),

deduplicated_participants AS (
    -- Deduplicates participant records, but in this cte if all participants for a session have appeared for a previous session, the new session might not show up
    SELECT *
    FROM (
        SELECT 
            pid,
            state_name,
            course_name,
            course_short_name,
            course_category,
            program_short_name,
            reg_attending_program,
            created_on,
            updated_on,
            -- Keep only the first record per `pid`
            ROW_NUMBER() OVER (
                PARTITION BY pid 
                ORDER BY COALESCE(
                    TO_DATE(updated_on, 'DD/MM/YYYY'), 
                    TO_DATE(created_on, 'DD/MM/YYYY')
                )
            ) AS row_num
        FROM {{ ref('participant_impact') }}
    ) filtered
    WHERE row_num = 1  -- Keeps only the first occurrence of each pid
),

participant_impact_agg AS (
    -- Ensures all sessions are counted and all valid participants are included with duplication across sessions
    SELECT
        COALESCE(s.state_name, d.state_name)::TEXT AS state_name,
        EXTRACT(YEAR FROM COALESCE(
            TO_DATE(d.updated_on, 'DD/MM/YYYY'), 
            TO_DATE(d.created_on, 'DD/MM/YYYY')
        )) AS year,
        COALESCE(s.course_name, d.course_name) AS course_name,
        COALESCE(s.course_short_name, d.course_short_name) AS course_short_name,
        COALESCE(s.course_category, d.course_category) AS course_category,
        COALESCE(s.program_short_name, d.program_short_name) AS program_short_name,
        COALESCE(s.reg_attending_program, d.reg_attending_program) AS participant_category,
        COUNT(DISTINCT d.pid) AS total_unique_participants,
        'participant_impact' AS source
    FROM full_session_list s
    FULL OUTER JOIN deduplicated_participants d
        ON s.course_name = d.course_name
        AND s.course_short_name = d.course_short_name
        AND s.course_category = d.course_category
        AND s.program_short_name = d.program_short_name
        AND s.reg_attending_program = d.reg_attending_program
        AND COALESCE(s.state_name, '') = COALESCE(d.state_name, '') -- Allows NULL state_name matching
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

no_registrations_agg AS (
    SELECT
        NULL::TEXT AS state_name,
        NULL::INTEGER AS year,
        course_name,
        course_short_name,
        course_category,
        program_short_name,
        participant_category,
        SUM(participant_count) AS total_unique_participants,
        'no_registrations' AS source
    FROM {{ ref('int_no_registration') }}
    GROUP BY 2, 3, 4, 5, 6, 7
)

-- Ensures we have all valid sessions and participants counted correctly
SELECT * FROM participant_impact_agg
UNION ALL
SELECT * FROM no_registrations_agg