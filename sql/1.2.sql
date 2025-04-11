--Query 2: Which age groups and road user roles are most associated with fatalities?
SELECT
  COALESCE(p.age_group, 'All Age Groups') AS age_group,   -- Replace NULLs for readability
  COALESCE(p.road_user, 'All Road Users') AS road_user,
  SUM(f.fatality_count) AS total_fatalities
FROM fact_person_fatality f
JOIN dim_person p ON f.person_id = p.person_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2024
GROUP BY CUBE (p.age_group, p.road_user)
HAVING GROUPING(p.age_group) + GROUPING(p.road_user) < 2  -- Exclude total summary row (All × All)
ORDER BY total_fatalities DESC;