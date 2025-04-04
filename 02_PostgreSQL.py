# PostgreSQL.py
from utility.pg_utils import create_table, insert_data, insert_many, query_data, drop_table
from utility.schemas import TABLE_SCHEMAS, TABLE_IMPORT_ORDER
import pandas as pd
from typing import List, Optional
import os


# Directory paths for input/output
OUTPUT_DIR = "output"
DB_files_export = "DB_files_export"


# ==========================
# Table Creation & Deletion
# ==========================

# Create all tables based on TABLE_IMPORT_ORDER and predefined schemas
def create_all_tables():
    for table in TABLE_IMPORT_ORDER:
        create_table(table, TABLE_SCHEMAS[table])

# Drop all tables in reverse order (to avoid foreign key constraint errors)
def drop_all_tables():
    for table in reversed(TABLE_IMPORT_ORDER):
        drop_table(table)


# ==========================
# Data Preparation & Insertion
# ==========================

# Replace NA values with None for PostgreSQL compatibility
def prepare_df_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    # 替换 pd.NA / np.nan → None（PostgreSQL 可识别）
    df = df.astype(object).where(pd.notnull(df), None)
    return df

# Insert a full DataFrame into a table
def insert_dataframe(table_name: str, df: pd.DataFrame):
    df = prepare_df_for_postgres(df)
    cols = ','.join(df.columns)
    placeholders = ','.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.to_numpy()]
    insert_many(insert_sql, data)


# ==========================
# Querying & Previewing
# ==========================

# Execute SELECT statement and convert results to DataFrame
def query_to_dataframe(select_sql: str, params: Optional[tuple] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
    results, inferred_columns = query_data(select_sql, params)

    if columns:
        df = pd.DataFrame(results, columns=columns)
    else:
        df = pd.DataFrame(results, columns=inferred_columns)

    
    print(df)
    print(f"📋 Converted query results to DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")
    return df

# Preview contents of all tables and optionally export them to CSV
def preview_all_tables(limit: int = None):
    for table_name in TABLE_SCHEMAS:
        print(f"\n📄 Previewing first {limit} rows of `{table_name}`:")
        try:
            select_sql = f"SELECT * FROM {table_name}"
            if limit:
                select_sql += f" LIMIT {limit}"
            results, columns = query_data(select_sql)

            if results:
                df = pd.DataFrame(results, columns=columns)

                os.makedirs(DB_files_export, exist_ok=True)
                df.to_csv(os.path.join(DB_files_export, f"{table_name}.csv"), index=False)
                print(df.head(15))
            else:
                print("⚠️ No data found in this table")
        except Exception as e:
            print(f"❌ Error while querying `{table_name}`: {e}")


# ==========================
# CSV Import
# ==========================


csv_headers = {}

# Import all CSVs in OUTPUT_DIR into corresponding database tables
def import_all_csv_to_db():
    for table_name in TABLE_IMPORT_ORDER:
        filename = f"{table_name}.csv"
        file_path = os.path.join(OUTPUT_DIR, filename)

        if not os.path.exists(file_path):
            print(f"⚠️ File `{filename}` not found, skipping.")
            continue

        print(f"\n📥 Importing `{filename}` into `{table_name}`...")
        try:
            df = pd.read_csv(file_path)
            csv_headers[table_name] = df.columns.tolist()
            insert_dataframe(table_name, df)
        except Exception as e:
            print(f"❌ Failed to import `{table_name}`: {e}")


# ==========================
# Run SQL Script File
# ==========================

# Execute a .sql script file with one or more queries
def run_sql_file(filename):
    df_results = {}

    with open(filename, "r", encoding="utf-8") as f:
        sql_content = f.read()

    statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
    for i, stmt in enumerate(statements, start=1):
        print(f"\n💡 Executing SQL #{i}:\n{stmt}")
        try:
            result, columns = query_data(stmt)
            df = pd.DataFrame(result, columns=columns)
            print(df)
            df_results[f"query_{i}"] = df
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            df_results[f"query_{i}"] = None

    return df_results


# ==========================
# Main Execution Block
# ==========================


def main():

    # Drop all existing tables
    drop_all_tables()

    # Delete one certain table，e.g. fact_person_fatality
    # drop_table("fact_person_fatality")

    # Create all tables
    create_all_tables()

    # Import CSV data to database
    import_all_csv_to_db()

    
    # Insert a DataFrame into a specific table
    # df = pd.read_csv("your_file.csv")
    # insert_dataframe("your_table_name", df)


    # Preview contents of all tables
    preview_all_tables(None)



    # Optional: Run manual queries
    # df = query_to_dataframe(
    #     "SELECT * FROM dim_road"
    #     # columns=["person_id", "age", "gender", "road_user", "age_group"]
    # )
    # print(df.head(20))


    # Running Business queries sql files
    # dfs = run_sql_file("sql/1.1.sql")
    # print(dfs["query_1"].head())


if __name__ == "__main__":
    main()

