
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
