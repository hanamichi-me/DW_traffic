#pg_utils.py
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
import pandas as pd



# ---------- 配置参数 ----------
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "project1",
    "user": "postgres",
    "password": "oddSt@mp92"
}

# ---------- 数据库连接 ----------
@contextmanager
def with_db_cursor():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("❌ 数据库操作失败:", e)
        raise
    finally:
        cur.close()
        conn.close()

# ---------- 创建表 ----------

def create_table(table_name: str, schema_sql: str):
    check_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
    """
    with with_db_cursor() as cur:
        cur.execute(check_sql, (table_name,))
        exists = cur.fetchone()[0]

        if exists:
            print(f"⚠️ 表 `{table_name}` 已存在，跳过创建")
        else:
            create_sql = f"CREATE TABLE {table_name} ({schema_sql});"
            cur.execute(create_sql)
            print(f"✅ 表 `{table_name}` 创建成功")


# ---------- 插入数据 ----------
def insert_data(insert_sql: str, data: tuple):
    with with_db_cursor() as cur:
        cur.execute(insert_sql, data)
        print("✅ 数据插入成功")

# ---------- 批量插入 ----------
def insert_many(insert_sql: str, data_list: list):
    with with_db_cursor() as cur:
        cur.executemany(insert_sql, data_list)
        print(f"✅ 批量插入 {len(data_list)} 条记录成功")

# ---------- 查询数据 ----------

def query_data(select_sql: str, params=None):
    with with_db_cursor() as cur:
        cur.execute(select_sql, params)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]  # 提取列名
        print(f"✅ 查询完成，共 {len(results)} 行")
        return results, columns


        
# ---------- 删除表 ----------
def drop_table(table_name: str):
    with with_db_cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        print(f"🗑️ 表 `{table_name}` 删除成功")


# ---------- 业务查询 ----------
def run_sql_file(filename):
    df_results = {}

    with open(filename, "r", encoding="utf-8") as f:
        sql_content = f.read()

    statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
    for i, stmt in enumerate(statements, start=1):
        print(f"\n💡 正在执行第 {i} 条 SQL：\n{stmt}")
        try:
            result, columns = query_data(stmt)
            df = pd.DataFrame(result, columns=columns)
            df_results[f"query_{i}"] = df
        except Exception as e:
            print(f"❌ 执行失败：{e}")
            df_results[f"query_{i}"] = None

    return df_results