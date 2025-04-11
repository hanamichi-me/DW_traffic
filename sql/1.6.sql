-- Query 6: How have fatalities from different types of crashes changed over time (by year) across different speed categories?

SELECT
d.year AS year,
COALESCE(c.crash_type, 'All Crash Types') AS crash_type,
COALESCE(r.speed_category, 'All Speed Zones') AS speed_category,
SUM(f.fatality_count) AS total_fatalities
FROM fact_person_fatality f
JOIN dim_crash_type c ON f.crash_type_id = c.crash_type_id
JOIN dim_road r ON f.road_id = r.road_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY CUBE (d.year, c.crash_type, r.speed_category)
HAVING GROUPING(c.crash_type) + GROUPING(r.speed_category) < 2
ORDER BY d.year ASC, total_fatalities DESC;
