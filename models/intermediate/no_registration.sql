{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        "courseId",
        "courseName",
        "endDateStr",
        "startDateStr",
        "courseCategory",
        "departmentName",
        "courseShortName",
        "programShortName",
        "unregisteredCount"  -- Case-sensitive reference
    FROM {{ source('source_ummeed_synergy_connect', 'no_registrations') }}
)

SELECT
    sd."courseId" AS course_id,
    sd."courseName" AS course_name,
    sd."endDateStr" AS end_date_str,
    sd."startDateStr" AS start_date_str,
    sd."courseCategory" AS course_category,
    sd."departmentName" AS department,
    sd."courseShortName" AS course_short_name,
    sd."programShortName" AS program_short_name,
    jsonb_extract_path_text(content.value, 'totalcount')::INTEGER AS total_count,
    jsonb_extract_path_text(content.value, 'participantCount')::INTEGER AS participant_count,
    jsonb_extract_path_text(content.value, 'participantCategory') AS participant_category
FROM source_data AS sd,
    LATERAL jsonb_array_elements(sd."unregisteredCount"->'content') AS content
