#pg_utils.py
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
import pandas as pd



# ---------- Configuration Parameters ----------
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "project1",
    "user": "postgres",
    "password": "oddSt@mp92"
}

# ---------- Database Connection Context Manager ----------
@contextmanager
def with_db_cursor():
    """
    Provides a managed database cursor with automatic connection handling.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("❌ Database operation failed:", e)
        raise
    finally:
        cur.close()
        conn.close()

# ---------- Table Creation ----------

def create_table(table_name: str, schema_sql: str):
    """
    Create a table if it does not already exist.
    """
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
            print(f"⚠️ Table  `{table_name}` already exists. Skipping creation.")
        else:
            create_sql = f"CREATE TABLE {table_name} ({schema_sql});"
            print(create_sql)
            cur.execute(create_sql)
            print(f"✅ Table  `{table_name}` created successfully.")


# ---------- Insert Single Row ----------
def insert_data(insert_sql: str, data: tuple):
    """
    Insert a single row into a table.
    """
    with with_db_cursor() as cur:
        cur.execute(insert_sql, data)
        print("✅ Row inserted successfully.")

# ---------- Insert Multiple Rows ----------
def insert_many(insert_sql: str, data_list: list):
    """
    Bulk insert multiple rows into a table.
    """
    with with_db_cursor() as cur:
        cur.executemany(insert_sql, data_list)
        print(f"✅ Successfully inserted {len(data_list)} rows.")


# ---------- Query Data ----------

def query_data(select_sql: str, params=None):
    """
    Execute a SELECT query and return the results and column names.
    """
    with with_db_cursor() as cur:
        cur.execute(select_sql, params)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]  # 提取列名
        print(f"✅ Query returned {len(results)} rows.")
        return results, columns


# ---------- Drop Table ----------
def drop_table(table_name: str):
    """
    Drop a table if it exists (including dependent objects).
    """
    with with_db_cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        print(f"🗑️ Table `{table_name}` dropped successfully.")


# ---------- Business-Specific Queries ----------
# (To be implemented or extended as needed)
