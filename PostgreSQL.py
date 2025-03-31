# PostgreSQL.py
from utility.pg_utils import create_table, insert_data, insert_many, query_data, drop_table, run_sql_file
from utility.schemas import TABLE_SCHEMAS
import pandas as pd
from typing import List, Optional
import os

OUTPUT_DIR = "output"
DB_files_export = "DB_files_export"

def create_all_tables():
    for table, schema in TABLE_SCHEMAS.items():
        create_table(table, schema)

def drop_all_tables():
    # 删除顺序要注意先删除 fact 表，再删 dim 表（避免外键冲突）
    # reverse 排序保证先删 fact 后删 dim
    for table in reversed(TABLE_SCHEMAS.keys()):
        drop_table(table)


def prepare_df_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    # 替换 pd.NA / np.nan → None（PostgreSQL 可识别）
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def insert_dataframe(table_name: str, df: pd.DataFrame):
    df = prepare_df_for_postgres(df)
    cols = ','.join(df.columns)
    placeholders = ','.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.to_numpy()]
    insert_many(insert_sql, data)


def query_to_dataframe(select_sql: str, params: Optional[tuple] = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
    results, inferred_columns = query_data(select_sql, params)

    if columns:
        df = pd.DataFrame(results, columns=columns)
    else:
        df = pd.DataFrame(results, columns=inferred_columns)

    
    print(df)
    print(f"📋 已将查询结果转换为 DataFrame，共 {len(df)} 行，列：{df.columns.tolist()}")
    return df


def preview_all_tables(limit: int = None):
    for table_name in TABLE_SCHEMAS:
        print(f"\n📄 表 `{table_name}` 的前 {limit} 行预览：")
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
                print("⚠️ 该表暂无数据")
        except Exception as e:
            print(f"❌ 查询表 `{table_name}` 出错：{e}")



csv_headers = {}

def import_all_csv_to_db():
    for filename in os.listdir(OUTPUT_DIR):
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            file_path = os.path.join(OUTPUT_DIR, filename)

            print(f"\n📥 正在导入 `{filename}` 到表 `{table_name}`...")
            try:
                df = pd.read_csv(file_path)
                csv_headers[table_name] = df.columns.tolist()  # 🆕 缓存列名
                insert_dataframe(table_name, df)
            except Exception as e:
                print(f"❌ 导入 `{table_name}` 失败: {e}")

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
            print(df)
            df_results[f"query_{i}"] = df
        except Exception as e:
            print(f"❌ 执行失败：{e}")
            df_results[f"query_{i}"] = None

    return df_results


def main():

    # 删除表
    # drop_all_tables()

    # # 删除某个表，比如 fact_person_fatality
    # drop_table("fact_person_fatality")


    # create_all_tables()

    # 👇 导入所有 CSV 到数据库
    # import_all_csv_to_db()

    
    # 添加数据
    # # 如果你只是添加新数据：
    # df = pd.read_csv("your_file.csv")
    # insert_dataframe("your_table_name", df)


    # 查询表
    # preview_all_tables(None)

    # df = query_to_dataframe(
    #     "SELECT * FROM fact_person_fatality"
    #     # columns=["person_id", "age", "gender", "road_user", "age_group"]
    # )
    # print(df.head(20))

    #======================
    dfs = run_sql_file("sql/1.1.sql")
    # 查看第一条查询结果
    # print(dfs["query_1"].head())


# 然后在 main 中执行
if __name__ == "__main__":
    main()

