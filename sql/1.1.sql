-- 查询各地区（LGA）的死亡人数（排除 Unknown）
SELECT 
    dl.lga_name,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_location dl ON fpf.location_id = dl.location_id
WHERE dl.lga_name != 'Unknown'
GROUP BY dl.lga_name
ORDER BY death_count DESC
LIMIT 10;

-- **哪些时间（月份/周几/时段）是致命事故高发期？**
SELECT 
    dd.month,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_date dd ON fpf.date_id = dd.date_id
GROUP BY dd.month
ORDER BY death_count DESC;

SELECT 
    dd.day_of_week_name,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_date dd ON fpf.date_id = dd.date_id
GROUP BY dd.day_of_week_name
ORDER BY 
    CASE 
        WHEN dd.day_of_week_name = 'Monday' THEN 1
        WHEN dd.day_of_week_name = 'Tuesday' THEN 2
        WHEN dd.day_of_week_name = 'Wednesday' THEN 3
        WHEN dd.day_of_week_name = 'Thursday' THEN 4
        WHEN dd.day_of_week_name = 'Friday' THEN 5
        WHEN dd.day_of_week_name = 'Saturday' THEN 6
        WHEN dd.day_of_week_name = 'Sunday' THEN 7
    END;

SELECT 
    dt.time_of_day,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_time dt ON fpf.time_of_day_id = dt.time_of_day_id
GROUP BY dt.time_of_day
ORDER BY death_count DESC;

SELECT 
    dd.month,
    dd.day_of_week_name,
    dt.time_of_day,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_date dd ON fpf.date_id = dd.date_id
JOIN dim_time dt ON fpf.time_of_day_id = dt.time_of_day_id
GROUP BY CUBE(dd.month, dd.day_of_week_name, dt.time_of_day)
ORDER BY death_count DESC;     

-- 3. **哪些类型的致命车祸（如：bus/重卡参与 vs 非参与）死亡人数最多？**
SELECT 
    dv.bus_involvement,
    dv.heavy_rigid_truck_involvement,
    dv.articulated_truck_involvement,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_vehicle dv ON fpf.vehicle_id = dv.vehicle_id
GROUP BY 
    dv.bus_involvement,
    dv.heavy_rigid_truck_involvement,
    dv.articulated_truck_involvement
ORDER BY death_count DESC;

-- 4. **每起事故平均死亡人数是否存在上升趋势？**
SELECT 
    dd.year,
    COUNT(DISTINCT fpf.crash_id) AS total_crashes,
    COUNT(*) AS total_deaths,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT fpf.crash_id), 2) AS avg_deaths_per_crash
FROM fact_person_fatality fpf
JOIN dim_date dd ON fpf.date_id = dd.date_id
GROUP BY dd.year
ORDER BY dd.year;