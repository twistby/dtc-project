{{ config(materialized="view") }}

select
    id,
    date,
    primary_type,
    description,
    concat(primary_type, ' ', description) as full_description,
    location_description,
    year,
    latitude,
    longitude
from {{ source("staging", "crimes") }}
where latitude != 0 and longitude != 0 and location_description = 'STREET'
