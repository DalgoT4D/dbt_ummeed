{% snapshot registered_mobile_no_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='mrno',
        strategy='check',
        check_cols=['registered_mobile_no']
    )
}}

select 
mrno,
registered_mobile_no
from {{ ref('clinic_data') }}

{% endsnapshot %}