SELECT
    airline
FROM agg_fact_flights.data
WHERE agg_level = 'airline'
    AND time_agg_level = 'year'