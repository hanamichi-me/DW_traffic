--Query 3: Which months have the highest number of fatal crashes, and is there a seasonal pattern? Would analyzing time of day enhance this insight?
SELECT
  d.month,
  COALESCE(t.time_of_day, 'All Times') AS time_of_day,   -- Label nulls in time_of_day
  SUM(f.fatality_count) AS total_fatalities
FROM fact_person_fatality f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_time t ON f.time_of_day_id = t.time_of_day_id
WHERE d.year = 2024
GROUP BY ROLLUP (d.month, t.time_of_day)                 -- Month → Time hierarchy
HAVING GROUPING(d.month) + GROUPING(t.time_of_day) < 2   -- Exclude grand total (null-null)
ORDER BY total_fatalities DESC;
