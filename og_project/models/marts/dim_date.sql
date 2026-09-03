WITH date_range AS (
    SELECT 
        MIN("Date")::date AS start_date,
        MAX("Date")::date AS end_date
    FROM {{ source('raw', 'og_sac_data') }}
),
date_spine AS(
    SELECT 
        generate_series(start_date, end_date, interval '1 day')::date AS trade_date
    FROM date_range
)

SELECT 
    trade_date,
    EXTRACT(DAY FROM trade_date) AS day,
    EXTRACT(MONTH FROM trade_date) AS month,
    EXTRACT(YEAR FROM trade_date) AS year,
    EXTRACT(DOW FROM trade_date) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM trade_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_spine
ORDER BY trade_date