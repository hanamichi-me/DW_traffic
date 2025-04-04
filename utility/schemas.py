# -*- coding: utf-8 -*-
# schemas.py
# ✅ Define table import order for CSV loading & table creation in ETL or database initialization.
TABLE_IMPORT_ORDER = [
    "dim_location",
    "dim_crash_type",
    "dim_date",
    "dim_holiday",
    "dim_person",
    "dim_time",
    "dim_vehicle",
    "dim_road",
    "fact_fatal_crash",
    "fact_person_fatality"
]

# ✅ Define table schemas for PostgreSQL creation using SQL DDL statements
TABLE_SCHEMAS = {
    # Dimension table for geographic location and population data
    "dim_location": """
        location_id SERIAL PRIMARY KEY,
        state VARCHAR(20),
        lga_name VARCHAR(100),
        sa4_name VARCHAR(100),
        remoteness_area VARCHAR(100),
        population_2023_lga INTEGER,
        population_2023_remoteness INTEGER,
        dwelling_records INTEGER
    """,

    # Dimension table for vehicle involvement types
    "dim_vehicle": """
        vehicle_id SERIAL PRIMARY KEY,
        bus_involvement VARCHAR(10),
        heavy_rigid_truck_involvement VARCHAR(10),
        articulated_truck_involvement VARCHAR(10)
    """,

    # Dimension table for crash type (e.g., Single, Multiple)
    "dim_crash_type": """
        crash_type_id SERIAL PRIMARY KEY,
        crash_type VARCHAR(20)
    """,

    # Dimension table for calendar attributes
    "dim_date": """
        date_id INTEGER PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        quarter INTEGER,
        day_of_week_name VARCHAR(20),
        day_type VARCHAR(10)
    """,

    # Dimension table for holiday periods (Christmas, Easter)
    "dim_holiday": """
        holiday_id INTEGER PRIMARY KEY,
        christmas_period BOOLEAN,
        easter_period BOOLEAN
    """,

    # Dimension table for personal attributes of individuals involved in crashes
    "dim_person": """
        person_id SERIAL PRIMARY KEY,
        age INTEGER,
        age_group VARCHAR(10),
        gender VARCHAR(20),
        road_user VARCHAR(50)
    """,

    # Dimension table for time of day classification
    "dim_time": """
        time_of_day_id SERIAL PRIMARY KEY,
        time_of_day VARCHAR(20)
    """,

    # Dimension table for road attributes
    "dim_road": """
        road_id SERIAL PRIMARY KEY,
        road_type VARCHAR(50),
        speed_limit INTEGER,
        speed_category VARCHAR(20)
    """,

    # Fact table summarizing each fatal crash event
    "fact_fatal_crash": """
        fact_crash_id INTEGER PRIMARY KEY,
        crash_id INTEGER,
        date_id INTEGER,
        holiday_id INTEGER,
        location_id INTEGER,
        road_id INTEGER,
        vehicle_id INTEGER,
        crash_type_id INTEGER,
        number_fatalities INTEGER,
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (holiday_id) REFERENCES dim_holiday(holiday_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (road_id) REFERENCES dim_road(road_id),
        FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
        FOREIGN KEY (crash_type_id) REFERENCES dim_crash_type(crash_type_id)
    """,

    # Fact table recording individual-level fatality data
    "fact_person_fatality": """
        fact_person_fatality_id SERIAL PRIMARY KEY,
        crash_id INTEGER,
        date_id INTEGER,
        holiday_id INTEGER,
        person_id INTEGER,
        location_id INTEGER,
        road_id INTEGER,
        vehicle_id INTEGER,
        crash_type_id INTEGER,
        time_of_day_id INTEGER,
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (holiday_id) REFERENCES dim_holiday(holiday_id),
        FOREIGN KEY (person_id) REFERENCES dim_person(person_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (road_id) REFERENCES dim_road(road_id),
        FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
        FOREIGN KEY (crash_type_id) REFERENCES dim_crash_type(crash_type_id),
        FOREIGN KEY (time_of_day_id) REFERENCES dim_time(time_of_day_id)
    """
}


