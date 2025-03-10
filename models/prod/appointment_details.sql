{{ config(materialized='table') }}

SELECT * FROM {{ref('appointment_data')}}