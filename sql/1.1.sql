 -- 1. 在 2024 年，不同路段类型在不同时间段的致死人数分别是多少？是否存在高发的“时段 × 路段”组合？
SELECT
    dt.time_of_day AS "Time of Day",
    dr.road_type AS "Road Type",
    COUNT(*) AS "Number of Fatalities"
FROM fact_person_fatality fpf
JOIN dim_time dt ON fpf.time_of_day_id = dt.time_of_day_id
JOIN dim_road dr ON fpf.road_id = dr.road_id
JOIN dim_date dd ON fpf.date_id = dd.date_id
WHERE dd.year = 2024
  AND dr.road_type <> 'Undetermined'
GROUP BY ROLLUP(dt.time_of_day, dr.road_type)
ORDER BY dt.time_of_day, COUNT(*) DESC;


-- 2. 年龄段 × 使用者角色 的致死分布
SELECT
    dp.age_group,
    dp.road_user,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_person dp ON fpf.person_id = dp.person_id
JOIN dim_date dd ON fpf.date_id = dd.date_id
WHERE dd.year = 2024
GROUP BY ROLLUP(dp.age_group, dp.road_user)
ORDER BY dp.age_group, death_count DESC;


-- 3. 州 × 偏远程度 的致死人数分布
SELECT
    dl.state,
    dl.remoteness_area,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_location dl ON fpf.location_id = dl.location_id
JOIN dim_date dd ON fpf.date_id = dd.date_id
WHERE dd.year = 2024
    AND dl.remoteness_area IS DISTINCT FROM 'Unknown'
GROUP BY CUBE(dl.state, dl.remoteness_area)
ORDER BY death_count DESC;

