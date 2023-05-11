with zones as (
    select
        {{ dbt_utils.generate_surrogate_key(['LocationID'])}} as zoneid,
        LocationID,
        Borough,
        Zone,
        replace(service_zone, 'Boro', 'Green') as Service_Zone

    from {{ ref('taxi_zone_loopup') }}
)

select * from zones
