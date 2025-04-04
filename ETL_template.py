# ETL_template.py

import pandas as pd
import os

# ========== CONFIG ==========
DATA_DIR = "sources"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== LOADERS ==========
def load_fatal_crash_data():
    df = pd.read_excel(os.path.join(DATA_DIR, "Fatal_Crashes_December_2024.xlsx"), sheet_name="BITRE_Fatal_Crash", skiprows=4)

    return df

def load_fatality_data():
    return pd.read_excel(os.path.join(DATA_DIR, "bitre_fatalities_dec2024.xlsx"), sheet_name="BITRE_Fatality", skiprows=4)

def load_dwelling_data():
    df = pd.read_csv(os.path.join(DATA_DIR, "LGA (count of dwellings).csv"), skiprows=11, header=None, usecols=[0, 1])
    df = df.iloc[:556].dropna().reset_index(drop=True)
    df.columns = ['LGA_EN', 'dwelling_records']
    df['LGA_EN'] = df['LGA_EN'].str.strip().str.replace('"', '')
    df['dwelling_records'] = df['dwelling_records'].astype(int)
    return df

def load_population_table(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(
        os.path.join(DATA_DIR, "Population_estimates.xlsx"),
        sheet_name=sheet_name,
        skiprows=6,
        header=None
    )

    # Set column names based on the first row
    df.columns = df.iloc[0]
    df.columns = (
        df.columns
        .str.replace(r"[^\w]+", "_", regex=True)  # Replace all non-alphanumeric/underscore characters with _
        .str.replace(" ", "_")
        .str.strip("_")  # Remove leading/trailing underscores
        .str.lower()
    )
    df = df.drop(index=0).reset_index(drop=True)

    # Ensure all column names are strings
    df.columns = df.columns.map(str)

    # Select the 2nd and last columns
    df = df.iloc[:-2, [1, -1]].copy()

    # Rename the last column to 'population_2023'
    df.columns.values[1] = 'population_2023'

    # Convert population values to integers
    df['population_2023'] = pd.to_numeric(df['population_2023'], errors='coerce').astype('Int64')

    # Remove rows where the first column (e.g., LGA name) contains "total" (case-insensitive)
    print(df[df.iloc[:, 0].str.contains("total", case=False, na=False)])
    df = df[~df.iloc[:, 0].str.contains("total", case=False, na=False)].reset_index(drop=True)

    return df



# ========== CLEANERS ==========


def common_clean_steps(df):

    df.columns = (
        df.columns
        .str.replace(r"[^\w]+", "_", regex=True)  # 非字母数字下划线的字符全部转为 _
        .str.replace(" ", "_")
        .str.strip("_")  # 去除开头结尾的 _
        .str.lower()
    )
    df = df.dropna(axis=1, how='all')

    # bool_columns = [
    #     'bus_involvement', 'heavy_rigid_truck_involvement',
    #     'articulated_truck_involvement', 'christmas_period', 'easter_period'
    # ]
    # for col in bool_columns:
    #     if col in df.columns:
    #         df[col] = df[col].map({'Yes': True, 'No': False, '-9': pd.NA, -9: pd.NA})



    
    if 'road_user' in df.columns:
        special_missing_count = (df['road_user'] == 'Other/-9').sum()
        if special_missing_count > 0:
            df['road_user'] = df['road_user'].replace('Other/-9', pd.NA)
            print(f"[Missing Data Handling] Replaced {special_missing_count} 'Other/-9' values with missing values in column `road_user`.")

    for col in df.columns:
        # 统计待替换的值数量
        replace_mask = df[col].isin(["Unknown", "nan","-9", -9])
        replace_count = replace_mask.sum()
        if replace_count > 0 :
            df[col] = df[col].replace(["Unknown", "nan","-9", -9], pd.NA)
            print(f"[Missing Data Handling] Replaced {replace_count} values ('Unknown', 'nan','-9', -9) with missing values in column `{col}`.")

    bool_columns = [
        'bus_involvement', 'heavy_rigid_truck_involvement',
        'articulated_truck_involvement', 'christmas_period', 'easter_period'
    ]
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].map({'Yes': True, 'No': False, pd.NA: "Unknown"})

    if 'speed_limit' in df.columns:
        special_case = df['speed_limit'] == '<40'
        if special_case.sum() > 0:
            df.loc[special_case, 'speed_limit'] = 40
            print(f"[Value Correction] Replaced {special_case.sum()} '<40' entries in `speed_limit` with 40.")

        df['speed_limit'] = pd.to_numeric(df['speed_limit'], errors='coerce')

        before = len(df)
        df = df[df['speed_limit'].notna()]
        removed = before - len(df)
        if removed > 0:
            print(f"[Clean Step] Removed {removed} rows with missing `speed_limit`.")

    # 通用字段缺失行删除逻辑
    for col in ['age', 'time_of_day']:
        if col in df.columns:
            before = len(df)
            df = df[df[col].notna()]
            removed = before - len(df)
            if removed > 0:
                print(f"[Clean Step] Removed {removed} rows with missing `{col}`.")

    # 将 lga_name, sa4_name, remoteness_area 中的 NA 替换为 'Unknown'
    for col in ['national_lga_name_2021', 'sa4_name_2021', 'national_remoteness_areas', 'gender', 'road_user']:
        if col in df.columns:
            missing = df[col].isna().sum()
            df[col] = df[col].fillna("Unknown")
            if missing > 0:
                print(f"[Location Normalization] Replaced {missing} missing values in `{col}` with 'Unknown'.")

    return df


# ========== DIM TABLES ==========

def generate_dim_time_of_day(fatal_crash_df):
    """
    Generate the unique Dim_TimeOfDay table based on the time_of_day column in fatal_crash_df.
    """
    # Extract unique time_of_day categories
    dim_time = fatal_crash_df[['time_of_day']].drop_duplicates().copy()


    # add primary id
    dim_time['time_of_day_id'] = range(1, len(dim_time) + 1)

    # reorder
    dim_time = dim_time[['time_of_day_id', 'time_of_day']]

    return dim_time


def generate_dim_person(fatality_df):
    # 1.Retain columns related to person attributes
    dim_person = fatality_df[['age', 'gender', 'road_user']].drop_duplicates().copy()


    # 2.Create age group categories
    dim_person['age_group'] = pd.cut(
        dim_person['age'],
        bins=[0, 17, 25, 40, 65, 200],
        labels=['0-17', '18-25', '26-40', '41-65', '65+'],
        right=False
    )

    # 3. add primary key ID
    dim_person = dim_person.reset_index(drop=True)
    dim_person['person_id'] = dim_person.index + 1

    # 4. reorder
    dim_person = dim_person[[
        'person_id',
        'age',
        'age_group',
        'gender',
        'road_user'
    ]]

    return dim_person


def generate_dim_date(fatal_crash_df):
    dim_date = fatal_crash_df[['year', 'month', 'dayweek', 'day_of_week', 'christmas_period', 'easter_period']].drop_duplicates().copy()


    dim_date = dim_date.rename(columns={
        'dayweek': 'day_of_week_name',
        'day_of_week': 'day_type'
    })

    dim_date['quarter'] = ((dim_date['month'] - 1) // 3 + 1)
    # 4. Construct surrogate key (artificial primary key)
    dim_date = dim_date.sort_values(by=['year', 'month', 'day_of_week_name']).reset_index(drop=True)
    dim_date['date_id'] = dim_date.index + 1

    dim_date = dim_date[[
        'date_id', 'year', 'month', 'quarter',
        'day_of_week_name',  # Monday, Tuesday...
        'day_type',          # Weekday / Weekend
        'christmas_period', 
        'easter_period'
    ]]

    return dim_date

def categorize_speed_limit(val):
    try:
        val = int(val)
        if val <= 50:
            return '≤50'
        elif 50 < val <= 80:
            return '51–80'
        elif 80 < val <= 100:
            return '81–100'
        else:
            return '100+'
    except:
        return pd.NA


def generate_dim_road(fatal_crash_df: pd.DataFrame, dim_speed_zone: pd.DataFrame) -> pd.DataFrame:
    dim_road = fatal_crash_df[['national_road_type', 'speed_limit']].drop_duplicates().copy()
    dim_road.columns = ['road_type', 'speed_limit']


    # Join with speed_zone_id
    dim_road = dim_road.merge(dim_speed_zone, on='speed_limit', how='left')

    dim_road['road_id'] = range(1, len(dim_road) + 1)
    dim_road = dim_road[['road_id', 'road_type', 'speed_limit', 'speed_zone_id']]

    return dim_road


def classify_speed_category(speed):
    try:
        speed = int(speed)
        if speed <= 30:
            return 'Very Low'       # 学区、步行街等
        elif speed <= 50:
            return 'Low'            # 居民区
        elif speed <= 70:
            return 'Medium'         # 城市主干道
        elif speed <= 100:
            return 'High'           # 国道/快速路
        else:
            return 'Very High'      # 高速公路
    except:
        return pd.NA

def generate_dim_speed_zone(fatal_crash_df: pd.DataFrame) -> pd.DataFrame:
    df = fatal_crash_df[['speed_limit']].dropna().copy()
    df = df.drop_duplicates().reset_index(drop=True)
    df['speed_category'] = df['speed_limit'].apply(classify_speed_category)
    df['speed_zone_id'] = range(1, len(df) + 1)

    df = df[['speed_zone_id', 'speed_limit', 'speed_category']]
    return df


def generate_dim_vehicle(fatal_crash_df):
    """
    Generate the Dim_Vehicle table by extracting whether
    the crash involved a bus, heavy rigid truck, or articulated truck.
    """
    vehicle_df = fatal_crash_df[[
        'bus_involvement',
        'heavy_rigid_truck_involvement',
        'articulated_truck_involvement'
    ]].drop_duplicates().reset_index(drop=True).copy()


    vehicle_df['vehicle_id'] = range(1, len(vehicle_df) + 1)

    # reorder
    vehicle_df = vehicle_df[[
        'vehicle_id',
        'bus_involvement',
        'heavy_rigid_truck_involvement',
        'articulated_truck_involvement'
    ]]

    return vehicle_df


def generate_dim_crash_type(fatal_crash_df):
    """
    Generate the Dim_CrashType table based on crash_type (e.g., Single / Multiple).
    """
    dim_crash_type = fatal_crash_df[['crash_type']].drop_duplicates().reset_index(drop=True).copy()
    dim_crash_type['crash_type_id'] = range(1, len(dim_crash_type) + 1)

    # reorder
    dim_crash_type = dim_crash_type[['crash_type_id', 'crash_type']]

    return dim_crash_type


def generate_dim_location(fatal_crash_df, lga_pop_df, sua_pop_df, remote_pop_df, dwelling_df):
    # 1. Extract raw geographic fields
    dim_location = fatal_crash_df[[
        'state',
        'national_lga_name_2021',
        'sa4_name_2021',
        'national_remoteness_areas'
    ]].drop_duplicates().copy()

    # 2. Standardize column names
    dim_location.columns = ['state', 'lga_name', 'sa4_name', 'remoteness_area']

    # 3. Join SUA name (can be mapped via SA4 if needed)
    # If you have an SUA column: dim_location['sua_name'] = ...
    # Skipped for now; can be added manually or imported from an external mapping

    # 4. Merge population data
    lga_pop_df = lga_pop_df.rename(columns={'local_government_area': 'lga_name'})
    sua_pop_df = sua_pop_df.rename(columns={'significant_urban_area': 'sua_name'})
    remote_pop_df = remote_pop_df.rename(columns={'remoteness_area': 'remoteness_area'})

    # Extract most recent population (2023)
    lga_pop_df = lga_pop_df[['lga_name', 'population_2023']]
    sua_pop_df = sua_pop_df[['sua_name', 'population_2023']]
    remote_pop_df = remote_pop_df[['remoteness_area', 'population_2023']]

    # Merge population data
    dim_location = dim_location.merge(lga_pop_df, on='lga_name', how='left').rename(columns={'population_2023': 'population_2023_lga'})
    dim_location = dim_location.merge(remote_pop_df, on='remoteness_area', how='left').rename(columns={'population_2023': 'population_2023_remoteness'})

    # 5. Merge dwelling data (2021)
    dwelling_df = dwelling_df.rename(columns={'LGA_EN': 'lga_name'})
    dim_location = dim_location.merge(dwelling_df, on='lga_name', how='left')

    # 6. Add surrogate key
    dim_location['location_id'] = range(1, len(dim_location) + 1)

    # Drop rows missing critical fields
    dim_location = dim_location.dropna(subset=['lga_name', 'sa4_name', 'remoteness_area'])

    # 7. Reorder columns
    dim_location = dim_location[[
        'location_id', 'state','lga_name', 'sa4_name', 'remoteness_area',
        'population_2023_lga', 'population_2023_remoteness', 'dwelling_records'
    ]]

    return dim_location




# ========== FACT TABLES ==========

def generate_fact_person_fatality(fatality_df, dim_person, dim_date, dim_location, dim_road, dim_vehicle, dim_crash_type, dim_time):
    """
    Construct the fact table:Fact_Person_Fatality
    """
    # rename for consistent joining
    df = fatality_df.rename(columns={
        'national_lga_name_2021': 'lga_name',
        'sa4_name_2021': 'sa4_name',
        'national_remoteness_areas': 'remoteness_area',
        'national_road_type': 'road_type'
    })

    # Merge: Dim_Location
    df = df.merge(
        dim_location[['location_id', 'state','lga_name', 'sa4_name', 'remoteness_area']],
        on=['state','lga_name', 'sa4_name', 'remoteness_area'],
        how='left'
    )

    # Merge: Dim_Person
    df = df.merge(dim_person, on=['age', 'gender', 'road_user'], how='left')

    # Merge: Dim_Date
    df = df.merge(
        dim_date[['date_id', 'year', 'month', 'day_of_week_name', 'day_type', 'christmas_period', 'easter_period']],
        left_on=['year', 'month', 'dayweek', 'day_of_week', 'christmas_period', 'easter_period'],
        right_on=['year', 'month', 'day_of_week_name', 'day_type', 'christmas_period', 'easter_period'],
        how='left'
    )


    # Merge: Dim_Road
    df = df.merge(
        dim_road[['road_id', 'road_type', 'speed_limit']],
        on=['road_type', 'speed_limit'],
        how='left'
    )

    # Merge: Dim_Vehicle
    df = df.merge(
        dim_vehicle[['vehicle_id', 'bus_involvement', 'heavy_rigid_truck_involvement', 'articulated_truck_involvement']],
        on=['bus_involvement', 'heavy_rigid_truck_involvement', 'articulated_truck_involvement'],
        how='left'
    )

    # Merge: Dim_CrashType
    df = df.merge(
        dim_crash_type[['crash_type_id', 'crash_type']],
        on='crash_type',
        how='left'
    )

    # Merge: Dim_TimeOfDay
    df = df.merge(
        dim_time[['time_of_day_id', 'time_of_day']],
        on='time_of_day',
        how='left'
    )

    # primary key
    df['fact_person_fatality_id'] = range(1, len(df) + 1)

    # Construct the final fact table
    fact_person_fatality = df[[
        'fact_person_fatality_id',
        'crash_id',
        'date_id',
        'person_id',
        'location_id',
        'road_id',
        'vehicle_id',
        'crash_type_id',
        'time_of_day_id'
    ]].copy()

    return fact_person_fatality


def generate_fact_fatal_crash(fatal_crash_df, dim_road, dim_vehicle, dim_crash_type, dim_location, dim_date):
    # Rename columns for consistent joining
    df = fatal_crash_df.rename(columns={
        'national_lga_name_2021': 'lga_name',
        'sa4_name_2021': 'sa4_name',
        'national_remoteness_areas': 'remoteness_area',
        'national_road_type': 'road_type'
    })

    # Merge: Dim_Location
    df = df.merge(
        dim_location[['location_id', 'state', 'lga_name', 'sa4_name', 'remoteness_area']],
        on=['state', 'lga_name', 'sa4_name', 'remoteness_area'],
        how='left'
    )

    # Merge: Dim_Road
    df = df.merge(
        dim_road[['road_id', 'road_type', 'speed_limit']],
        on=['road_type', 'speed_limit'],
        how='left'
    )

    # Merge: Dim_Vehicle
    df = df.merge(
        dim_vehicle[['vehicle_id', 'bus_involvement', 'heavy_rigid_truck_involvement', 'articulated_truck_involvement']],
        on=['bus_involvement', 'heavy_rigid_truck_involvement', 'articulated_truck_involvement'],
        how='left'
    )

    # Merge: Dim_CrashType
    df = df.merge(
        dim_crash_type[['crash_type_id', 'crash_type']],
        on='crash_type',
        how='left'
    )

    df = df.merge(
        dim_date[['date_id', 'year', 'month', 'day_of_week_name', 'day_type', 'christmas_period', 'easter_period']],
        left_on=['year', 'month', 'dayweek', 'day_of_week', 'christmas_period', 'easter_period'],
        right_on=['year', 'month', 'day_of_week_name', 'day_type', 'christmas_period', 'easter_period'],
        how='left'
    )


    # Primary key (directly using crash_id)
    df['fact_crash_id'] = df['crash_id']

    # Construct the final fact table
    fact_fatal_crash = df[[
        'fact_crash_id',
        'crash_id',
        'date_id',
        'location_id',
        'road_id',
        'vehicle_id',
        'crash_type_id',
        'number_fatalities'
    ]].copy()

    return fact_fatal_crash



# ========== SAVE FUNCTION ==========

def save_table(df, name):

    # Handle pd.NA in boolean columns by converting to object and replacing missing values with None
    bool_cols = df.select_dtypes(include="boolean").columns.tolist()
    for col in bool_cols:
        df[col] = df[col].astype(object).where(df[col].notna(), None)

    # Convert all columns to object type and replace NaN / pd.NA with None (PostgreSQL-compatible)
    df = df.astype(object)
    df = df.where(pd.notnull(df), None)

    # Save the DataFrame to CSV
    df.to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)

# ========== MAIN FUNCTION ==========
def main():
    raw_fatal_crash_df = load_fatal_crash_data()
    fatal_crash_df = common_clean_steps(raw_fatal_crash_df)

    raw_fatality_df = load_fatality_data()
    fatality_df = common_clean_steps(raw_fatality_df)


    dwelling_df = load_dwelling_data()

    lga_pop_df = load_population_table("Table 1")
    sua_pop_df = load_population_table("Table 2")
    remote_pop_df = load_population_table("Table 3")
    ced_pop_df = load_population_table("Table 4")

    dim_location = generate_dim_location(fatal_crash_df, lga_pop_df, sua_pop_df, remote_pop_df, dwelling_df)
    save_table(dim_location, "dim_location")

    dim_speed_zone = generate_dim_speed_zone(fatal_crash_df)
    save_table(dim_speed_zone, "dim_speed_zone")

    dim_road = generate_dim_road(fatal_crash_df, dim_speed_zone)
    save_table(dim_road, "dim_road")

    dim_vehicle = generate_dim_vehicle(fatal_crash_df)
    save_table(dim_vehicle, "dim_vehicle")

    dim_crash_type = generate_dim_crash_type(fatal_crash_df)
    save_table(dim_crash_type, "dim_crash_type")

    dim_date = generate_dim_date(fatal_crash_df)
    save_table(dim_date, "dim_date")

    fact_fatal_crash = generate_fact_fatal_crash(fatal_crash_df, dim_road, dim_vehicle, dim_crash_type, dim_location, dim_date)
    save_table(fact_fatal_crash, "fact_fatal_crash")

    dim_person = generate_dim_person(fatality_df)
    save_table(dim_person, "dim_person")

    dim_time = generate_dim_time_of_day(fatal_crash_df)
    save_table(dim_time, "dim_time")

    fact_person_fatality = generate_fact_person_fatality(fatality_df, dim_person, dim_date, dim_location, dim_road, dim_vehicle, dim_crash_type, dim_time)
    save_table(fact_person_fatality, "fact_person_fatality")


if __name__ == "__main__":
    main()
