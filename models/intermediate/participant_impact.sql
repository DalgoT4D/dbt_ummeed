{{ config(materialized='table') }}

SELECT
    "pId" AS pid, 
    city,
    gender AS participant_gender,
    taluka,
    "departmentName" AS department,
    "CreatedOn" AS created_on,
    "UpdatedOn" AS updated_on,
    "stateName" AS state_name,
    "courseName" AS course_name,
    TO_DATE("endDateStr", 'DD/MM/YYYY') AS end_date,
    speciality,
    "totalChild" AS total_child,
    "workerRole" AS worker_role,
    "countryName" AS country_name,
    designation,
    participant,
    "totalMember" AS total_member,
    "yearOfBirth" AS year_of_birth,
    "countryCode1" AS country_code1,
    "countryCode2" AS country_code2,
    "districtName" AS district_name,
    TO_DATE("startDateStr", 'DD/MM/YYYY') AS start_date,
    TO_CHAR(TO_DATE("startDateStr", 'DD/MM/YYYY'), 'Month') AS month,
    "educatopnRole" AS education_role,
    "regPatientId1" AS reg_patient_id1,
    "regPatientId2" AS reg_patient_id2,
    "regPatientId3" AS reg_patient_id3,
    "childNameinfo1" AS child_name_info1,
    "childNameinfo2" AS child_name_info2,
    "childNameinfo3" AS child_name_info3,
    "courseCategory" AS course_category,
    "primaryContact"::VARCHAR AS primary_contact,
    "ummeedService1" AS ummeed_service1,
    "ummeedService2" AS ummeed_service2,
    "ummeedService3" AS ummeed_service3,
    "courseShortName" AS course_short_name,
    "orgnisationName" AS organisation_name,
    "regMonthWorking" AS reg_month_working,
    "takeCareOfChild" AS take_care_of_child,
    childgenderinfo1 AS child_gender_info1,
    childgenderinfo2 AS child_gender_info2,
    childgenderinfo3 AS child_gender_info3,
    organisationwork AS organisation_work,
    "programShortName" AS program_short_name,
    "secondaryContact" AS secondary_contact,
    "relationWithChild" AS relation_with_child,
    "regNonWorkingMonth" AS working_with_people_devdelay_montly,
    CASE
        WHEN LOWER("regNonWorkingMonth") = 'none' THEN 0

        WHEN LOWER("regNonWorkingMonth") LIKE 'greater than %' THEN 
            CAST(
                REGEXP_REPLACE(LOWER("regNonWorkingMonth"), '^greater than\s+(\d+).*$', '\1')
                AS INTEGER
            )

        WHEN "regNonWorkingMonth" LIKE '%-%' THEN ROUND(
            (
                CAST(LTRIM(SPLIT_PART(TRIM("regNonWorkingMonth"), '-', 1), '0') AS INTEGER) +
                CAST(LTRIM(SPLIT_PART(TRIM("regNonWorkingMonth"), '-', 2), '0') AS INTEGER)
            ) / 2.0
        )::INTEGER

        ELSE 0
    END::INTEGER AS training_indirect_reach_monthly,
    "childBirthDateinfo1" AS child_birth_date_info1,
    "childBirthDateinfo2" AS child_birth_date_info2,
    "childBirthDateinfo3" AS child_birth_date_info3,
    "parentQualification" AS parent_qualification,
    recipantservicetext AS recipient_service_text,
    "regAttendingProgram" AS reg_attending_program,
    "sSelectEorkVolunteer" AS select_work_volunteer,
    "childPrimaryDiagnosis1" AS child_primary_diagnosis1,
    "childPrimaryDiagnosis2" AS child_primary_diagnosis2,
    "childPrimaryDiagnosis3" AS child_primary_diagnosis3,
    "monthlyHouseholdIncome" AS monthly_household_income,
    mailconfirmregistration AS mail_confirm_registration,
    contactedbyemailinfuture AS contacted_by_email_in_future,
    permissioninterviewedpost AS permission_interviewed_post,
    permissiontousephotovideos AS permission_to_use_photo_videos,
    whereyouhearaboutprogcourtext AS where_you_hear_about_prog_course_text
FROM {{ source('source_ummeed_synergy_connect', 'participant_impact') }}
