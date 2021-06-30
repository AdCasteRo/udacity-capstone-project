import configparser




# CONFIG
config = configparser.ConfigParser()
config.read('config.cfg')

ACCESS_KEY_ID=config['AWS']['AWS_ACCESS_KEY_ID']
SECRET_ACCESS_KEY=config['AWS']['AWS_SECRET_ACCESS_KEY']

IMMI_DATA        = 's3://acr-udacity-capstone-bucket-2/immi'
TEMP_DATA        = "s3://acr-udacity-capstone-bucket-2/temp"
DEMO_DATA        = "s3://acr-udacity-capstone-bucket-2/demo"
AIRP_DATA        = "s3://acr-udacity-capstone-bucket-2/airp"
TIME_DATA        = "s3://acr-udacity-capstone-bucket-2/time"




# DROP TABLES

staging_immigration_table_drop = "DROP TABLE IF EXISTS staging_i94data;"
staging_temperature_table_drop = "DROP TABLE IF EXISTS staging_temp;"
staging_airport_table_drop = "DROP TABLE IF EXISTS staging_airp;"
staging_demographics_table_drop = "DROP TABLE IF EXISTS staging_demo;"

immigration_table_drop = "DROP TABLE IF EXISTS immigration;"
temperature_table_drop = "DROP TABLE IF EXISTS temperature;"
airport_table_drop = "DROP TABLE IF EXISTS airport;"
demographics_table_drop = "DROP TABLE IF EXISTS demographics;"
time_table_drop = "DROP TABLE IF EXISTS time;"




# CREATE FINAL TABLES

immigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration (
        id INT8 PRIMARY KEY SORTKEY DISTKEY, 
        arrival_date DATE NOT NULL,
        departure_date DATE NOT NULL,
        birth_country TEXT NOT NULL,
        residence_country TEXT NOT NULL,
        port TEXT NOT NULL,
        age INT,
        birth_year INT,
        visa TEXT,
        gender CHAR,
        mode TEXT,
        state TEXT,
        type TEXT
    );
""")

temperature_table_create = ("""
    CREATE TABLE IF NOT EXISTS temperature (
        id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
        country TEXT NOT NULL,
        city TEXT NOT NULL,
        date INT NOT NULL,
        average_temp FLOAT,
        uncertanty FLOAT
    );
""")

airport_table_create = ("""
    CREATE TABLE IF NOT EXISTS airport (
        id TEXT PRIMARY KEY SORTKEY DISTKEY,
        type TEXT,
        name TEXT,
        elevation INT,
        continent TEXT,
        country TEXT,
        region TEXT,
        municipality TEXT,
        iata_code TEXT
    );
""")

demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS demographics (
        id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
        median_age FLOAT,
        male_population INT,
        female_population INT,
        total_population INT,
        number_veterans INT,
        foreign_born INT,
        average_household FLOAT,
        state_code TEXT,
        race TEXT,
        count INT,
        state TEXT,
        city TEXT

    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time DATE PRIMARY KEY SORTKEY,
        year INT,
        month INT,
        dayOfWeek INT,
        dayOfMonth INT,
        dayOfYear INT,
        weekOfYear INT
    )DISTSTYLE ALL;
""")




#LOADING THE DATA INTO REDSHIFT

immigration_copy = ("""
    COPY immigration FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'  
    FORMAT AS PARQUET;
""").format(
    IMMI_DATA,ACCESS_KEY_ID, SECRET_ACCESS_KEY
)

temperature_copy = ("""
    COPY temperature FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}' 
    FORMAT AS PARQUET;
""").format(
    TEMP_DATA,ACCESS_KEY_ID, SECRET_ACCESS_KEY
)

demographics_copy = ("""
    COPY demographics FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'     
    FORMAT AS PARQUET;
""").format(
    DEMO_DATA,ACCESS_KEY_ID, SECRET_ACCESS_KEY
)

airport_copy = ("""
    COPY airport FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}' 
    FORMAT AS PARQUET;
""").format(
    AIRP_DATA,ACCESS_KEY_ID, SECRET_ACCESS_KEY
)

time_copy = ("""
    COPY time FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}' 
    FORMAT AS PARQUET;
""").format(
    TIME_DATA,ACCESS_KEY_ID, SECRET_ACCESS_KEY
)




# CAST TABLES

immi_cast = ("""
        SELECT 
                BIGINT(cicid),
                date_add('1960-01-01', arrdate) as arrdates,
                date_add('1960-01-01', depdate) as depdates,
                birth.Country as BirthCountry,
                res.Country as ResidenceCountry,
                Port,
                INT(i94bir) as age,
                INT(biryear) as biryear,
                visatype,
                gender,
                Mode,
                State,
                Type
        FROM immi
        LEFT JOIN cit_res AS birth ON immi.i94cit = birth.code
        LEFT JOIN cit_res AS res ON immi.i94res = res.code
        LEFT JOIN port ON immi.i94port = port.code
        LEFT JOIN mode ON immi.i94mode = mode.code
        LEFT JOIN addr ON immi.i94addr = addr.code
        LEFT JOIN visa ON immi.i94visa = visa.code
    """)

temp_cast = ("""
    SELECT
        INT('0') as ID,
        Country,
        City,
        to_date(dt, 'yyyy-mm-dd') as dt,
        DOUBLE(AverageTemperature) as AverageTemperature,
        DOUBLE(AverageTemperatureUncertainty) as Uncertainty
    FROM temp
""")

demo_cast = ("""
    SELECT
        INT('0') as ID,
        DOUBLE(Median_age) as median_age,
        INT(Male_population) as male_population,
        INT(Female_population) as female_population,
        INT(Total_population) as total_population,
        INT(Number_of_Veterans) as number_of_veterans,
        INT('Foreign-born') as foreign_born,
        DOUBLE(Average_Household_Size) as average_household_size,
        State_Code,
        Race,
        INT(Count) as count,
        State,
        City
    FROM demo
""")

airp_cast = ("""
    SELECT
        ident,
        type,
        name,
        INT(elevation_ft) as elevation,
        continent,
        iso_country,
        iso_region,
        municipality,
        iata_code
    FROM airp
""")




# QUALITY QUERIES

immi_quality = ("""
    SELECT
        COUNT(*)
    FROM immigration
""")

temp_quality = ("""
    SELECT
        COUNT(*)
    FROM immigration
""")

demo_quality = ("""
    SELECT
        COUNT(*)
    FROM immigration
""")

airp_quality = ("""
    SELECT
        COUNT(*)
    FROM immigration
""")

time_quality = ("""
    SELECT
        COUNT(*)
    FROM immigration
""")


# QUERY LISTS

drop_table_queries = [staging_immigration_table_drop, staging_temperature_table_drop, staging_airport_table_drop, staging_demographics_table_drop, immigration_table_drop, temperature_table_drop, airport_table_drop, demographics_table_drop, time_table_drop ]
create_table_queries = [ time_table_create, immigration_table_create, temperature_table_create, airport_table_create, demographics_table_create ]
copy_table_queries = [ airport_copy, time_copy,  temperature_copy, immigration_copy, demographics_copy ]
cast_table_queries = [ immi_cast, temp_cast, demo_cast, airp_cast ]
quality_table_queries = [ immi_quality, temp_quality, demo_quality, airp_quality, time_quality ]

