-- 2. **哪些时间（月份/周几/时段）是致命事故高发期？**
SELECT 
    dd.month,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_date dd ON fpf.date_id = dd.date_id
GROUP BY dd.month
ORDER BY death_count DESC;