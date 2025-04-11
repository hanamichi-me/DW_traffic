--Query 4: During holidays such as Christmas and Easter, which road types and time periods see more fatal crashes?
SELECT
  h.holiday_id,
  COALESCE(r.road_type, 'All Road Types') AS road_type,     -- Label NULLs for clarity
  COALESCE(t.time_of_day, 'All Times') AS time_of_day,      -- Label NULLs for clarity
  SUM(f.fatality_count) AS total_fatalities
FROM fact_person_fatality f
JOIN dim_holiday h ON f.holiday_id = h.holiday_id
JOIN dim_road r ON f.road_id = r.road_id
JOIN dim_time t ON f.time_of_day_id = t.time_of_day_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2024 AND h.holiday_id IN (1, 3)               -- Only include Christmas and Easter
GROUP BY CUBE (h.holiday_id, r.road_type, t.time_of_day)
HAVING GROUPING(h.holiday_id) + GROUPING(r.road_type) + GROUPING(t.time_of_day) < 3  -- Remove full grand total
ORDER BY total_fatalities DESC;