# -*- coding: utf-8 -*-
# 03_Association_Rule_Mining.py
# Association Rule Mining using apriori algorithm for traffic fatality dataset

import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
import os

# ===============================
# Load CSV files into dictionary
# ===============================
def load_csv_files(folder_path):
    csv_data = {}
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            df = pd.read_csv(os.path.join(folder_path, filename))
            csv_data[table_name] = df
    return csv_data

# ================================================
# Select relevant columns based on user settings
# ================================================
def get_selected_columns(speed= "limit", preference="easter", vehicle="bus"):  
    selected_cols = [
        'gender', 'age_group',
        'road_type',
        'time_of_day',
        'crash_type',
        'day_type', 'road_user', 'state'
    ]

    if speed == "limit":
        selected_cols.append('speed_limit') 
    if speed == "category":
        selected_cols.append('speed_category')

    # Add holiday columns
    if preference == "easter":
        selected_cols.append('easter_period')
    if preference == "christmas":
        selected_cols.append('christmas_period')

    # Add vehicle columns
    if vehicle == "bus":
        selected_cols.append('bus_involvement')
    if vehicle == "heavy":
        selected_cols.append('heavy_rigid_truck_involvement')
    if vehicle == "articulated":
        selected_cols.append('articulated_truck_involvement')

    return selected_cols

# ===================================================
# Merge all dimension tables to construct transactions
# ===================================================
def prepare_transactions_custom(data, selected_cols):

    df = data["fact_person_fatality"].merge(data["dim_person"], on="person_id") \
        .merge(data["dim_date"], on='date_id') \
        .merge(data["dim_holiday"], on='holiday_id') \
        .merge(data["dim_location"], on='location_id') \
        .merge(data["dim_road"], on='road_id') \
        .merge(data["dim_vehicle"], on='vehicle_id') \
        .merge(data["dim_crash_type"], on='crash_type_id') \
        .merge(data["dim_time"], on='time_of_day_id') 
    
    
    # Transform selected columns to "col=value" format for association mining
    df = df[selected_cols]
    df = df.apply(lambda col: col.map(lambda x: f"{col.name}={x}" if pd.notnull(x) else pd.NA))
    transactions = df.apply(lambda row: row.dropna().tolist(), axis=1)

    # Encode transactions for mlxtend
    te = TransactionEncoder()
    df_encoded = te.fit(transactions).transform(transactions)
    return pd.DataFrame(df_encoded, columns=te.columns_)


# ========================================
# Run apriori + filter for specific target
# ========================================
def mine_association_rules(df_trans, target_rhs="road_user=", min_support=0.02, min_confidence=0.60, min_lift=1.0, top_k=10):
    freq_items = apriori(df_trans, min_support=min_support, use_colnames=True)
    rules = association_rules(freq_items, metric="lift", min_threshold=min_lift)

    # Keep only rules with a single consequent that starts with the target (e.g. road_user=)
    rules = rules[
        rules['consequents'].apply(lambda x: len(x) == 1 and list(x)[0].startswith(target_rhs))
    ]

    rules = rules[rules['confidence'] >= min_confidence]
    rules = rules.sort_values(by=["lift", "confidence"], ascending=False).head(top_k)


    return rules

# ==========================
# Format frozensets for CSV
# ==========================
def clean_frozenset_columns(df):
    def format_items(fset):
        if isinstance(fset, str):
            fset = eval(fset.replace("frozenset", "").strip("()"))
        sorted_items = sorted(fset)
        return "{" + ", ".join(sorted_items) + "}"

    df['antecedents'] = df['antecedents'].apply(format_items)
    df['consequents'] = df['consequents'].apply(format_items)
    return df


# ===================
# Main execution flow
# ===================
def main():
    data = load_csv_files("DB_files_export")
    all_rules = []

    # Define all parameter combinations to explore
    combinations = [
        ("limit","easter", "bus"),
        ("limit","easter", "heavy"),
        ("limit","easter", "articulated"),
        ("limit","christmas", "bus"),
        ("limit","christmas", "heavy"),
        ("limit","christmas", "articulated"),
        ("category","easter", "bus"),
        ("category","easter", "heavy"),
        ("category","easter", "articulated"),
        ("category","christmas", "bus"),
        ("category","christmas", "heavy"),
        ("category","christmas", "articulated")
    ]

    # Perform rule mining for each combination
    for speed, holiday, vehicle in combinations:
        print(f"🚀 combination：{speed} +{holiday} + {vehicle}")
        selected_cols = get_selected_columns(speed= speed, preference=holiday, vehicle=vehicle)
        df_trans = prepare_transactions_custom(data, selected_cols)
        rules = mine_association_rules(df_trans, target_rhs="road_user=",top_k=50)
        rules["combo"] = f"{speed}_{holiday}_{vehicle}"  # Label rule origin
        all_rules.append(rules)

    # Merge and clean all rules
    combined_rules = pd.concat(all_rules, ignore_index=True)

    # Export top rules
    final_top_k = combined_rules.sort_values(by=["lift", "confidence"], ascending=False)


    output_dir = "output_rules"
    os.makedirs(output_dir, exist_ok=True)
    final_top_k["support"] = final_top_k["support"].round(3)
    final_top_k["confidence"] = final_top_k["confidence"].round(3)
    final_top_k["lift"] = final_top_k["lift"].round(3)
    final_top_k = clean_frozenset_columns(final_top_k)
    export_cols = [ "antecedents", "consequents", "support", "confidence", "lift"]
    final_top_k = final_top_k[export_cols]
    final_top_k = final_top_k.drop_duplicates()
    final_top_k = final_top_k.head(50)
    output_path = os.path.join(output_dir, "combined_top10_rules.csv")
    final_top_k.to_csv(output_path, index=False)
    print("✅ Export the merged top K rules：{output_path}")

if __name__ == "__main__":
    main()
