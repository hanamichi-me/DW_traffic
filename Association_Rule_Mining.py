

import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
import os
from tqdm import tqdm


def load_csv_files(folder_path):
    csv_data = {}
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            df = pd.read_csv(os.path.join(folder_path, filename))
            csv_data[table_name] = df
    return csv_data


def merge_tables_for_arm(data):
    df = data["fact_person_fatality"].copy()

    # 多维度 Join
    df = df.merge(data["dim_person"], on="person_id", how="left")
    df = df.merge(data["dim_road"], on="road_id", how="left")
    df = df.merge(data["dim_vehicle"], on="vehicle_id", how="left")
    df = df.merge(data["dim_location"], on="location_id", how="left")
    df = df.merge(data["dim_date"], on="date_id", how="left")
    df = df.merge(data["dim_crash_type"], on="crash_type_id", how="left")
    df = df.merge(data["dim_time"], on="time_of_day_id", how="left")

    # 选择要参与挖掘的字段
    selected_fields = [
        "age_group", "gender", "road_type", "speed_limit_group",
        "bus_involvement", "heavy_rigid_truck_involvement", "articulated_truck_involvement",
        "state", "remoteness_area", "day_of_week_name", "day_type",
        "christmas_period", "easter_period", "crash_type", "time_of_day", "road_user"
    ]

    df = df[selected_fields]

    # 将布尔型字段转为字符串，避免 OneHot 编码时被忽略
    bool_cols = ["bus_involvement", "heavy_rigid_truck_involvement", "articulated_truck_involvement",
                 "christmas_period", "easter_period"]
    for col in bool_cols:
        df[col] = df[col].astype(str)

    # 删除含缺失的记录
    df = df.dropna()

    return df


# def prepare_transactions(df_person, df_date, df_location, df_road, df_vehicle, df_crash_type, df_time):
#     # 合并所有相关表
#     df = df_person.merge(df_date, on='date_id', how='left') \
#                   .merge(df_location, on='location_id', how='left') \
#                   .merge(df_road, on='road_id', how='left') \
#                   .merge(df_vehicle, on='vehicle_id', how='left') \
#                   .merge(df_crash_type, on='crash_type_id', how='left') \
#                   .merge(df_time, on='time_of_day_id', how='left')

#     # 选择有意义的字段（注意都是分类变量）
#     selected_cols = [
#         'age_group', 'gender', 'road_user',
#         'state', 'remoteness_area',
#         'road_type', 'speed_limit_group',
#         'bus_involvement', 'heavy_rigid_truck_involvement', 'articulated_truck_involvement',
#         'crash_type', 'time_of_day',
#         'day_of_week_name', 'day_type',
#         'christmas_period', 'easter_period'
#     ]

#     df = df[selected_cols]

#     # 把每列都变成“字段=值”这种格式，比如 gender=Male
#     df = df.apply(lambda col: col.map(lambda x: f"{col.name}={x}" if pd.notnull(x) else pd.NA))

#     # 每一行变成一个事务（每行是一个列表）
#     transactions = df.apply(lambda row: row.dropna().tolist(), axis=1)

#     # 用 TransactionEncoder 做 one-hot 编码
#     from mlxtend.preprocessing import TransactionEncoder
#     te = TransactionEncoder()
#     df_encoded = te.fit(transactions).transform(transactions)
#     df_encoded = pd.DataFrame(df_encoded, columns=te.columns_)

#     print(f"✅ 成功构造 {len(df_encoded)} 条事务，包含 {len(df_encoded.columns)} 个唯一项。")
#     return df_encoded

tqdm.pandas(desc="🚀 处理中")

def prepare_transactions(df_person, df_date, df_location, df_road, df_vehicle, df_crash_type, df_time):
    # 合并所有相关表
    df = df_person.merge(df_date, on='date_id', how='left') \
                  .merge(df_location, on='location_id', how='left') \
                  .merge(df_road, on='road_id', how='left') \
                  .merge(df_vehicle, on='vehicle_id', how='left') \
                  .merge(df_crash_type, on='crash_type_id', how='left') \
                  .merge(df_time, on='time_of_day_id', how='left')

    # 选择有意义的字段（注意都是分类变量）
    selected_cols = [
        'age_group', 'gender', 'road_user',
        'state', 'remoteness_area',
        'road_type', 'speed_limit_group',
        'bus_involvement', 'heavy_rigid_truck_involvement', 'articulated_truck_involvement',
        'crash_type', 'time_of_day',
        'day_of_week_name', 'day_type',
        'christmas_period', 'easter_period'
    ]

    df = df[selected_cols]

    # 将每列值转换为“字段=值”形式，例如 "gender=Male"
    df = df.apply(lambda col: col.map(lambda x: f"{col.name}={x}" if pd.notnull(x) else pd.NA))

    # 每一行变成一个事务（List of string），使用 tqdm 展示处理进度
    transactions = df.progress_apply(lambda row: row.dropna().tolist(), axis=1)

    # One-hot 编码转换为布尔特征矩阵
    te = TransactionEncoder()
    df_encoded = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(df_encoded, columns=te.columns_)

    print(f"\n✅ 成功构造 {len(df_encoded)} 条事务，包含 {len(df_encoded.columns)} 个唯一项。")
    return df_encoded




def mine_association_rules(df_trans, target_rhs="road_user=", min_support=0.05, min_confidence=0.60, min_lift=1.0, top_k=10):
    freq_items = apriori(df_trans, min_support=min_support, use_colnames=True)
    rules = association_rules(freq_items, metric="lift", min_threshold=min_lift)

    # 仅保留右侧只包含一个 road_user=xxx 的规则
    rules = rules[
        rules['consequents'].apply(lambda x: len(x) == 1 and list(x)[0].startswith(target_rhs))
    ]

    # confidence 较高，按 lift 和 confidence 排序
    rules = rules[rules['confidence'] >= min_confidence]
    # rules = rules.sort_values(by=["lift", "confidence"], ascending=False).head(top_k)
    rules = rules.sort_values(by=["lift", "confidence"], ascending=False).head(top_k)


    return rules



# def mine_association_rules(df_trans, target_rhs="road_user=", min_support=0.06, min_confidence=0.60, min_lift=2.0, top_k=10):
#     print("🔍 正在生成频繁项集 ...")
#     freq_items = fpgrowth(df_trans, min_support=min_support, use_colnames=True)

#     print("🧠 正在挖掘关联规则 ...")
#     rules = association_rules(freq_items, metric="lift", min_threshold=min_lift)

#     rules = rules[
#         rules['consequents'].apply(lambda x: len(x) == 1 and list(x)[0].startswith(target_rhs))
#     ]
#     rules = rules[rules['confidence'] >= min_confidence]
#     rules = rules.sort_values(by=[ "lift", "confidence"], ascending=False).head(top_k)

#     return rules




def main():
    data = load_csv_files("DB_files_export")

    df_trans = prepare_transactions(
        df_person=data["fact_person_fatality"].merge(data["dim_person"], on="person_id"),
        df_date=data["dim_date"],
        df_location=data["dim_location"],
        df_road=data["dim_road"],
        df_vehicle=data["dim_vehicle"],
        df_crash_type=data["dim_crash_type"],
        df_time=data["dim_time"]
    )

    rules = mine_association_rules(df_trans, target_rhs="road_user=")

    print("📋 Top Rules with 'road_user' on the RHS:")
    print(rules[["antecedents", "consequents", "support", "confidence", "lift"]])

    # ✅ 导出到 output_rules 文件夹
    output_dir = "output_rules"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "road_user_rules.csv")
    rules.to_csv(output_path, index=False)
    print(f"✅ 已保存关联规则到文件: {output_path}")



if __name__ == "__main__":
    main()
