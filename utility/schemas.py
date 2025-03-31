# schemas.py
TABLE_SCHEMAS = {
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

    "dim_road": """
        road_id SERIAL PRIMARY KEY,
        road_type VARCHAR(50),
        speed_limit INTEGER,
        speed_limit_group VARCHAR(10)
    """,

    "dim_vehicle": """
        vehicle_id SERIAL PRIMARY KEY,
        bus_involvement BOOLEAN,
        heavy_rigid_truck_involvement BOOLEAN,
        articulated_truck_involvement BOOLEAN
    """,

    "dim_crash_type": """
        crash_type_id SERIAL PRIMARY KEY,
        crash_type VARCHAR(20)
    """,

    "dim_date": """
        date_id INTEGER PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        quarter INTEGER,
        day_of_week_name VARCHAR(20),
        day_type VARCHAR(10),
        christmas_period BOOLEAN,
        easter_period BOOLEAN
    """,

    "dim_person": """
        person_id SERIAL PRIMARY KEY,
        age INTEGER,
        age_group VARCHAR(10),
        gender VARCHAR(20),
        road_user VARCHAR(50)
    """,

    "dim_time": """
        time_of_day_id SERIAL PRIMARY KEY,
        time_of_day VARCHAR(20)
    """,

    "fact_fatal_crash": """
        fact_crash_id INTEGER PRIMARY KEY,
        crash_id INTEGER,
        date_id INTEGER,
        location_id INTEGER,
        road_id INTEGER,
        vehicle_id INTEGER,
        crash_type_id INTEGER,
        number_fatalities INTEGER,
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (road_id) REFERENCES dim_road(road_id),
        FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
        FOREIGN KEY (crash_type_id) REFERENCES dim_crash_type(crash_type_id)
    """,

    "fact_person_fatality": """
        fact_person_fatality_id SERIAL PRIMARY KEY,
        crash_id INTEGER,
        date_id INTEGER,
        person_id INTEGER,
        location_id INTEGER,
        road_id INTEGER,
        vehicle_id INTEGER,
        crash_type_id INTEGER,
        time_of_day_id INTEGER,
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (person_id) REFERENCES dim_person(person_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (road_id) REFERENCES dim_road(road_id),
        FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
        FOREIGN KEY (crash_type_id) REFERENCES dim_crash_type(crash_type_id),
        FOREIGN KEY (time_of_day_id) REFERENCES dim_time(time_of_day_id)
    """
}


