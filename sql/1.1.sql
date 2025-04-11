-- Query 1: Select time of day and road type (with labels for aggregated rows), and compute total fatalities
SELECT
  COALESCE(t.time_of_day, 'All Times') AS time_of_day,    -- Replace NULL (from CUBE) with 'All Times' for readability
  COALESCE(r.road_type, 'All Road Types') AS road_type,    -- Replace NULL (from CUBE) with 'All Road Types'
  SUM(f.fatality_count) AS total_fatalities               -- Aggregate total fatalities
FROM fact_person_fatality f
-- Join with dimension tables to bring in descriptive attributes
JOIN dim_time t ON f.time_of_day_id = t.time_of_day_id
JOIN dim_road r ON f.road_id = r.road_id
JOIN dim_date d ON f.date_id = d.date_id
-- Filter for the year 2024 only
WHERE d.year = 2024
-- Use CUBE to generate all combinations and subtotal groupings
GROUP BY CUBE (t.time_of_day, r.road_type)
-- Exclude the grand total row (i.e., All Times × All Road Types)
HAVING GROUPING(t.time_of_day) + GROUPING(r.road_type) < 2
-- Sort the results in descending order of fatalities
ORDER BY total_fatalities DESC;
