{{ config(
    materialized="table"
) }}

WITH source_data AS (
    SELECT 
        "courseId" as course_id,
        "courseName" as course_name,
        "courseCategory" as course_category,
        "departmentName" as department_name,
        "courseShortName" as course_short_name,
        "programShortName" as program_short_name,
        ("unregisteredCount"->'content')::jsonb AS content_jsonb
    FROM {{ source('source_ummeed_synergy_connect', 'no_registrations') }} 
), 

unnested AS (
    SELECT
        s.course_id,
        s.course_name,
        s.course_category,
        s.department_name,
        s.course_short_name,
        s.program_short_name,
        c.value->>'totalcount' AS total_count,
        (c.value->>'participantCount')::int AS participant_count,
        c.value->>'participantCategory' AS participant_category
    FROM source_data s, 
    LATERAL jsonb_array_elements(s.content_jsonb) AS c
)

SELECT * FROM unnested