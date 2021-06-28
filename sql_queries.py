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



# CREATE STAGING TABLES

staging_immigration_table_create= ("""
     CREATE TABLE IF NOT EXISTS staging_i94data (
         cicid  DOUBLE PRECISION PRIMARY KEY,
         i94yr  INT,
         i94mon  INT,
         i94cit  DOUBLE PRECISION,
         i94res  DOUBLE PRECISION,
         i94port  TEXT,
         arrdate  DOUBLE PRECISION,
         i94mode  DOUBLE PRECISION,
         i94addr  TEXT,
         depdate  DOUBLE PRECISION,
         i94bir  DOUBLE PRECISION,
         i94visa  DOUBLE PRECISION,
         count  DOUBLE PRECISION,
         dtadfile  TEXT,
         visapost  TEXT,
         occup  TEXT,
         entdepa  TEXT,
         entdepd  TEXT,
         entdepu  TEXT,
         matflag  TEXT,
         biryear  DOUBLE PRECISION,
         dtaddto  TEXT,
         gender  TEXT,
         insnum  TEXT,
         airline  TEXT,
         admnum  DOUBLE PRECISION,
         fltno  TEXT,
         visatype  TEXT 
     );
""")

staging_temperature_table_create = ("""
     CREATE TABLE IF NOT EXISTS staging_temp (
        id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY, 
        dt TEXT,
        AverageTemperature TEXT,
        AverageTemperatureUncertainty TEXT,
        City TEXT,
        Country TEXT,
        Latitude TEXT,
        Longitude TEXT
     );
""")


staging_airport_table_create = ("""
     CREATE TABLE IF NOT EXISTS staging_airp (
        ident TEXT PRIMARY,
        type TEXT,
        name TEXT,
        elevation_ft TEXT,
        continent TEXT,
        iso_country TEXT,
        iso_region TEXT,
        municipality TEXT,
        gps_code TEXT,
        iata_code TEXT,
        local_code TEXT,
        coordinates TEXT
    );
""")


# CREATE FINAL TABLES

immigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigration (
        id INT PRIMARY KEY SORTKEY DISTKEY, 
        arrival_date INT NOT NULL,
        departure_date INT NOT NULL,
        birth_country INT NOT NULL,
        residence_country INT NOT NULL,
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
        id INT PRIMARY KEY SORTKEY DISTKEY,
        country TEXT NOT NULL,
        city TEXT NOT NULL,
        date INT NOT NULL,
        average_temp FLOAT,
        uncertanty FLOAT
    );
""")

airport_table_create = ("""
    CREATE TABLE IF NOT EXISTS airport (
        id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
        type TEXT,
        name TEXT,
        elevation INT,
        continent TEXT,
        country TEXT,
        region TEXT,
        municipality TEXT,
        iata_code TEXT,
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION
    );
""")

demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS demographics (
        id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
        city TEXT,
        state TEXT,
        median_age FLOAT,
        male_population INT,
        female_population INT,
        total_population INT,
        number_veterans INT,
        foreign_born INT,
        average_household FLOAT,
        state_code TEXT,
        race TEXT,
        count INT
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




#DEBUG QUERY
check_errors = ("""
    SELECT * FROM stl_load_errors
""")








# QUERY LISTS

drop_table_queries = [staging_immigration_table_drop, staging_temperature_table_drop, staging_airport_table_drop, staging_demographics_table_drop, immigration_table_drop, temperature_table_drop, airport_table_drop, demographics_table_drop, time_table_drop ]
create_table_queries = [ immigration_table_create, temperature_table_create, airport_table_create, demographics_table_create, time_table_create ]
copy_table_queries = [ immigration_copy, temperature_copy, demographics_copy, airport_copy, time_copy ]


errors_query = [check_errors]

