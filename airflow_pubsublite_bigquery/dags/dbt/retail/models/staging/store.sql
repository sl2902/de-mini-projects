{{ 
    config(materialized='table') 
}}

with base as (
    SELECT
        store_id,
        location,
        size,
        manager
    FROM
       {{ source('staging', 'store_2') }}
)

SELECT
    *
FROM
    base