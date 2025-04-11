# DW_traffic  
Data warehousing traffic accident analysis  
=======

## Part 1. Configuration

### 1. Create and activate a virtual environment (conda recommended)

```bash
conda create -n traffic_env python=3.10
conda activate traffic_env
```

### 2. Install dependencies
Project dependencies are listed in requirements.txt. To install them, run:

```bash
pip install -r requirements.txt
```


## Part 2. Run the ETL process
This file is responsible for reading the resource files from the resource folder, performing ELT, and exporting the results to the output folder.
```
python 01_ETL_template.py
```

## Part 3. Run the PostgreSQL process
This file is responsible for creating tables in your pre-existing database. 
It will import all the tables from the output folder into your database, and also includes some code for viewing SQL queries.

```
python 02_PostgreSQL.py
```

### 1. Create a database named `"project1"` and establish the connection. Configuration parameters are located in:
Update the settings in the utility/pg_utils.py file with your own information to connect to your database.

```python
# utility/pg_utils.py
# ---------- Configuration ----------
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "project1",
    "user": "postgres",
    "password": "oddSt@mp92"
}
```

### 2. Create all tables:
Steps 2 to 7 below are all commented out in the main function. To run a specific functionality, simply uncomment the corresponding line of code.
```
    # PostgreSQL.py
    create_all_tables()
```

### 3. Import all data (from CSV files):

```python
# PostgreSQL.py
import_all_csv_to_db()
```

### 4. Preview all tables:

```python
    # PostgreSQL.py
    preview_all_tables(None)  # You can specify a row limit; use None to show all rows
```

### 5. Query a specific table:

```python
    # PostgreSQL.py
df = query_to_dataframe(
    "SELECT * FROM fact_person_fatality"
    # Optional: specify columns
    # columns=["person_id", "age", "gender", "road_user", "age_group"]
)
    print(df.head(20))
```

### 6. Drop all tables (use with caution):

```python
    # PostgreSQL.py
    drop_all_tables()
```

### 7. Add new business queries:

```python
# PostgreSQL.py
dfs = run_sql_file("sql/1.1.sql")
```

## Part 4. Mining 
```
python 03_Association_Rule_Mining.py
```
Due to the need to explore multiple parameter combinations and perform high-dimensional transaction encoding, the mining process may take several minutes to complete. This reflects a deliberate trade-off between computational cost and the breadth of pattern discovery.


## Part 5. Tableau
To run Tableau, first make sure it is connected to your database. Also, ensure that the folder name for this directory is Project1_ETL. Additionally, make sure the GeoJSON files (mentioned in the project introduction page) are placed inside this directory. Note that we did not include this folder in our submission due to its large file size.

