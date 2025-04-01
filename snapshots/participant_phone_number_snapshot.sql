{% snapshot phone_numbers_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key='pid',
            strategy='check',
            check_cols=['primary_contact']
        )
    }}

    select
        pid,
        primary_contact
    from {{ ref('participant_impact') }}

{% endsnapshot %}