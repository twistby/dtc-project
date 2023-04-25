{{ config(materialized="table") }}

SELECT 
  c.id AS crime_id,
  c.date,
  c.primary_type,
  c.description,
  c.full_description,
  c.year,
  s.school_id AS school_id,
  s.short_name,
  s.address,
  ST_DISTANCE(ST_GeogPoint(c.longitude, c.latitude), ST_GeogPoint(s.long, s.lat)) AS distance,
  s.grade_cat
FROM 
  {{ ref("stg_street_crimes") }}  c
JOIN 
  {{ source("staging", "schools") }}  s 
ON 
  ST_DISTANCE(ST_GeogPoint(c.longitude, c.latitude), ST_GeogPoint(s.long, s.lat)) < 500
ORDER BY 
  c.id, 
  distance 