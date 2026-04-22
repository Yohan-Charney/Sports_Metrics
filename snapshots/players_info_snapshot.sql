{% snapshot players_info_snapshot %}

{{
    config(
        target_schema='Sport_Metrics',
        unique_key='player_id',
        strategy='check',
        check_cols=['Position', 'Height_cm', 'Weight_kg', 'Age'],
    )
}}

select * from {{ ref('Players_info') }}

{% endsnapshot %}