
# DW_traffic
Data warehousing traffic accident analysis
=======
1. 第一步:执行ETL, 运行

    ```python
    python PostgreSQL.py
    ```



2. 创建一个数据库"project1",并连接. 配置参数在

    ```python
    # utility\pg_utils.py
    # ---------- 配置参数 ----------
    DB_CONFIG = {
        "host": "localhost",
        "port": "5432",
        "database": "project1",
        "user": "postgres",
        "password": "oddSt@mp92"
    }
    ```



3. 建表

    ```python
    # PostgreSQL.py
    create_all_tables()
    ```



4. 导入数据

    ```python
    # PostgreSQL.py
    import_all_csv_to_db()
    ```



5. 查询所有表

    ```python
    # PostgreSQL.py
    preview_all_tables(None)	#可以输入预览行数, None显示为所有行
    ```



6. 查询某个表

    ```python
    # PostgreSQL.py
        df = query_to_dataframe(
            "SELECT * FROM fact_person_fatality"
            # columns=["person_id", "age", "gender", "road_user", "age_group"]
        )
        print(df.head(20))
    ```



7. 删除所有表

    ```python
    # PostgreSQL.py
    drop_all_tables()
    ```



8. 新增业务查询语句. 在sql文件夹里面, 写入你想查询的sql语句, 然后执行以下查询结果.

    ```python
    # PostgreSQL.py
    dfs = run_sql_file("sql/1.1.sql")
    ```



## business questions

非常棒的澄清 👏！你说得对 ——
 **像年份 (`year`)、季度 (`quarter`)、月份 (`month`)、州 (`state`) 等字段应该是作为“筛选器（Filter）”，而不是参与 `GROUP BY CUBE()` 或 `ROLLUP()` 的维度字段。**

------

### ✅ 所以我们来重新整理你要的版本（**更聚焦，更业务化，更视觉化友好**）：

------

## 🎯 1. 不同时段 × 道路类型 下致死人数分析

- **业务问题**：在哪些时间段与哪种类型的道路上，发生的致命事故最多？
- **可视化建议**：堆叠柱状图（Time of Day 作为主 X 轴，Road Type 为颜色分类）
- **SQL 示例**（可添加 Filter: `WHERE year = 2024`）：

```sql
SELECT
    dt.time_of_day,
    dr.road_type,
    COUNT(*) AS num_fatalities
FROM fact_person_fatality fpf
JOIN dim_time dt ON fpf.time_of_day_id = dt.time_of_day_id
JOIN dim_road dr ON fpf.road_id = dr.road_id
JOIN dim_date dd ON fpf.date_id = dd.date_id
WHERE dd.year = 2024
  AND dr.road_type IS NOT NULL
  AND dr.road_type <> 'Undetermined'
GROUP BY ROLLUP(dt.time_of_day, dr.road_type)
ORDER BY dt.time_of_day, num_fatalities DESC;
```

------

## 🎯 2. 年龄段 × 使用者角色 的致死分布

- **业务问题**：哪个年龄段、什么角色（如行人、驾驶人）致死人数最多？
- **可视化建议**：横向条形图（Age Group 为 Y 轴，角色为颜色分类）
- **SQL 示例**：

```sql
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
```

------

## 🎯 3. 州 × 偏远程度 的致死人数分布

- **业务问题**：在哪些地区（州 + 偏远等级）致死人数更高？
- **可视化建议**：地图图层 + 表格、分组条形图（州为主分类）
- **SQL 示例**：

```sql
SELECT
    dl.state,
    dl.remoteness_area,
    COUNT(*) AS death_count
FROM fact_person_fatality fpf
JOIN dim_location dl ON fpf.location_id = dl.location_id
JOIN dim_date dd ON fpf.date_id = dd.date_id
WHERE dd.year = 2024
GROUP BY CUBE(dl.state, dl.remoteness_area)
ORDER BY death_count DESC;
```

------

## 🎯 4. 节假日 × 时段 × 道路类型 的致死组合分析

- **业务问题**：节假日期间，在哪些时段、哪些道路更容易发生致死事故？
- **可视化建议**：热力图、交叉表
- **SQL 示例**：

```sql
SELECT
    dd.christmas_period,
    dt.time_of_day,
    dr.road_type,
    COUNT(*) AS fatalities
FROM fact_person_fatality fpf
JOIN dim_date dd ON fpf.date_id = dd.date_id
JOIN dim_time dt ON fpf.time_of_day_id = dt.time_of_day_id
JOIN dim_road dr ON fpf.road_id = dr.road_id
WHERE dd.year = 2024
  AND dr.road_type <> 'Undetermined'
GROUP BY CUBE(dd.christmas_period, dt.time_of_day, dr.road_type)
ORDER BY fatalities DESC;
```

------

## 🎯 5. 不同车辆组合的致死人数

- **业务问题**：涉及公交、大型卡车、挂车的事故中，哪种组合最危险？
- **可视化建议**：交叉表或堆叠条形图（每种组合一类）
- **SQL 示例**：

```sql
SELECT
    dv.bus_involvement,
    dv.heavy_rigid_truck_involvement,
    dv.articulated_truck_involvement,
    COUNT(*) AS deaths
FROM fact_person_fatality fpf
JOIN dim_vehicle dv ON fpf.vehicle_id = dv.vehicle_id
JOIN dim_date dd ON fpf.date_id = dd.date_id
WHERE dd.year = 2024
GROUP BY ROLLUP(dv.bus_involvement, dv.heavy_rigid_truck_involvement, dv.articulated_truck_involvement)
ORDER BY deaths DESC;
```

------

### ✅ 总结：设计理念

| 类型             | 示例维度组合                | 作为 Filter 的维度 |
| ---------------- | --------------------------- | ------------------ |
| CUBE/ROLLUP 内部 | time_of_day, road_type      | year, state        |
|                  | age_group, road_user        | year               |
|                  | state, remoteness_area      | year               |
|                  | christmas_period, road_type | year               |
|                  | 各种车辆参与情况            | year               |

------

我们可以现在从你最想做的那一个开始做 Tableau 可视化，我可以继续一步一步教你如何设计 Dashboard 或图表。告诉我从哪一个开始 😎
