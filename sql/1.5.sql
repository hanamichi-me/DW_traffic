-- Query5: Which Australian states had the highest fatality rates in 2023?

-- Step 1: Extract unique LGA-level population records (to avoid double-counting)
WITH unique_lga_population AS (
    SELECT DISTINCT
        state,                  -- Australian state or territory name
        lga_name,              -- Local Government Area name
        population_2023_lga    -- 2023 population estimate for this LGA
    FROM dim_location
),

-- Step 2: Aggregate the LGA populations to get total state-level population
state_population AS (
    SELECT
        state,
        SUM(population_2023_lga) AS total_population  -- Sum up all LGAs in the state
    FROM unique_lga_population
    GROUP BY state
)

-- Step 3: Join crash data with population data to compute death rate per 100,000
SELECT
    l.state,                                          -- State name
    SUM(f.number_fatalities) AS total_deaths_2023,   -- Total number of deaths in 2023
    p.total_population,                              -- Total population for the state
    ROUND(
        (SUM(f.number_fatalities) * 100000.0 / NULLIF(p.total_population, 0))::numeric, 2
    ) AS death_rate_per_100k_2023                     -- Deaths per 100,000 population
FROM fact_fatal_crash f
JOIN dim_location l ON f.location_id = l.location_id        -- Match crash to location
JOIN dim_date d ON f.date_id = d.date_id                    -- Filter crashes by date
JOIN state_population p ON l.state = p.state                -- Join with aggregated population
WHERE d.year = 2023                                         -- Filter for year 2023
GROUP BY l.state, p.total_population
ORDER BY death_rate_per_100k_2023 DESC;
