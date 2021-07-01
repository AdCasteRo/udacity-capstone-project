import pandas as pd
from pandas import melt
from pyspark.sql.functions import *
import re
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, immi_cast, temp_cast, airp_cast, demo_cast, quality_table_queries, integrity_table_queries
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sqlalchemy import create_engine
import time as chrono


# Step 0 Exploration

def integrity(df):
    """
    Returns the integrity (non-empty values) of each column of a given dataframe.
    
    param: dataframe
    returns: dataframe
    """
    count_df = df.select([count(c).alias(c) for c in df.columns]).toPandas()
    count_df = pd.melt(count_df, var_name='Column', value_name='Values')
    total = df.count()
    count_df['% integrity'] = 100*count_df['Values']/total
    count_df = count_df.sort_values('% integrity')
    
    return count_df


# Step 1 Pyspark

def spark_init():
    """
    Return a Spark Session with the adecuate parameters.
    
    Params: None
    Return: SparkSession
    """
    config = configparser.ConfigParser()
    config.read('config.cfg')
    
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']).config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']).config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false").getOrCreate()
    print("Spark Session created")
    
    return spark


def create_dataframe(spark):
    """
    Returns 4 dataframes created with spark with the information.
    
    params: SparkSession
    returns: 4x Spark DataFrames
    """
    
    i94data_df = spark.read.load('./sas_data')
    c1 = i94data_df.count()
    print("i94data_df created, {} rows.".format(c1))
    
    temp_df = spark.read.option("header", True).csv('GlobalLandTemperaturesByCity.csv')
    c2 = temp_df.count()
    print("temp_df created, {} rows.".format(c2))
    
    demo_df = spark.read.options(header = True, delimiter=';').csv('us-cities-demographics.csv')
    c3 = demo_df.count()
    print("demo_df created, {} rows.".format(c3))
    
    airp_df = spark.read.options(header = True).csv('airport-codes_csv.csv')
    c4 = airp_df.count()
    print("airp_df created , {} rows.".format(c4))
    
    return i94data_df, temp_df, demo_df, airp_df


def clean_dataframe(spark, immi_df, temp_df, demo_df, airp_df):
    """
    Drop not needed columns, delete duplicates, filter useless data, replace i94data codes with text names.
    
    Params: Spark session, 4 x Spark Dataframe
    Returns: 4 x Spark Dataframe
    """
      
    del_cols1 = ['entdepu', 'occup', 'insnum', 'visapost']
    immi_df = immi_df.drop(*del_cols1)
    
    del_cols2 = ['coordinates', 'Latitude', 'Longitude']
    airp_df = airp_df.drop(*del_cols2)
        
    del_cols2 = ['local_code', 'gps_code']
    airp_df = airp_df.drop(*del_cols2)
    
    col1 = (['cicid'])
    immi_df = d_duplicates(immi_df, col1)

    col2 = (['ident'])
    airp_df = d_duplicates(airp_df, col2)
    
    airp_df = airp_df.filter(airp_df.iata_code.isNotNull())
    
    temp_df = temp_df.filter(temp_df.Country == 'United States')
    temp_df = temp_df.filter(temp_df.AverageTemperature != 'None')
    
    for name in demo_df.schema.names:
          demo_df = demo_df.withColumnRenamed(name, name.replace(' ', '_'))
    
    return immi_df, temp_df, demo_df, airp_df


def d_duplicates(df, col):
    """
    Returns the dataframe without the duplicates according to the column and shows the number of rows deleted.
    
    param: dataframe, column
    returns: dataframe
    """

    drop_df = df.dropDuplicates(col)
    c1 = df.count()
    c2 = drop_df.count()
    
    c = c1-c2
    print(f"Number of duplicated rows: {c}")
    return drop_df


def cast_dataframe(spark, immi_df, temp_df, demo_df, airp_df):
    """
    Substitutes immi code fields with text values and casts all columns to the right data type.
    
    Params: SparkSession, 4 x Spark Dataframe
    Returns: 4 x Spark Dataframes
    """
    
    i94cit_res, i94mode, i94addr, i94visa = labels_reader()
    i94cit_res = spark.createDataFrame(i94cit_res)
    i94mode = spark.createDataFrame(i94mode)
    i94addr = spark.createDataFrame(i94addr)
    i94visa = spark.createDataFrame(i94visa)
    i94cit_res.createOrReplaceTempView("cit_res")
    i94mode.createOrReplaceTempView("mode")
    i94addr.createOrReplaceTempView("addr")
    i94visa.createOrReplaceTempView("visa")
 
    
    immi_df.createOrReplaceTempView("immi")
    temp_df.createOrReplaceTempView("temp")
    demo_df.createOrReplaceTempView("demo")
    airp_df.createOrReplaceTempView("airp")
    
    immi_df = spark.sql(immi_cast)
    temp_df = spark.sql(temp_cast)
    demo_df = spark.sql(demo_cast)
    airp_df = spark.sql(airp_cast)
    
    return immi_df, temp_df, demo_df, airp_df


def labels_reader():
    """
    Reads and maps the file I94_SAS_Labels_Descriptions.SAS to return the dimension tables defined in the file.
    Original source code: https://knowledge.udacity.com/questions/125439
    
    params: None
    Returns: 5 x DataFrame
    """
    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    
    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business', '2': 'Pleasure', '3' : 'Student'}
    
    i94cit_res_df = pd.DataFrame(list(i94cit_res.items()), columns=['Code', 'Country'])         
    i94mode_df = pd.DataFrame(list(i94mode.items()), columns=['Code', 'Mode'])   
    i94addr_df = pd.DataFrame(list(i94addr.items()), columns=['Code', 'State'])   
    i94visa_df = pd.DataFrame(list(i94visa.items()), columns=['Code', 'Type'])   
    
    
    return i94cit_res_df, i94mode_df, i94addr_df, i94visa_df  


def code_mapper(file, idx):
    """
    Returns a dict of values after the index word.
    Original source code: https://knowledge.udacity.com/questions/125439
    
    params: file, index
    returns: dic
    """
    f_content2 = file[file.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic


def create_time(spark, immi_df, temp_df):
    """
    Using Spark, gets the date information from the immigration table and temperature table and creates the time table.
    
    Params: SparksSession, 2 x Spark Dataframe
    Returns: Spark Dataframe
    
    """
    
    immi_df.createOrReplaceTempView("immi_df")
    temp_df.createOrReplaceTempView("temp_df")
        
    time_df = spark.sql("""
        SELECT arrdates as time FROM immi_df 
        UNION SELECT depdates as time FROM immi_df 
        UNION SELECT dt as time FROM temp_df
    """)
    
    time_df = time_df.select( to_date(col("time"),"yyyy-mm-dd").alias('time') , 
        year(col("time")).alias("year"), 
       month(col("time")).alias("month"), 
       dayofweek(col("time")).alias("dayofweek"), 
       dayofmonth(col("time")).alias("dayofmonth"), 
       dayofyear(col("time")).alias("dayofyear"),
       weekofyear(col("time")).alias("weekofyear") 
   )
    
    print("Time table created")
    
    return time_df


# Step 2 S3

def upload_s3(output_data, spark, immi, temp, demo, airp, time):
    """
    Upload all the tables to S3.
    
    Params: Output S3 host, Spark Session, 5 x Spark Dataframe
    Returns: Array
    """
    
    print("Starting upload to S3")
    
    temp.printSchema()
    start_time = chrono.time()
    #Change the selected columns below to just upload a sample.
    temp.write.mode('overwrite').parquet(output_data+'temp')
    #temp.limit(5).write.mode('overwrite').parquet(output_data+'temp')
    nt = temp.count()
    print("Temperature data uploaded, %s seconds" % (chrono.time() - start_time))
    
    immi.printSchema()
    start_time = chrono.time()
    #Change the selected columns below to just upload a sample.
    immi.write.mode('overwrite').parquet(output_data+"immi")
    #immi.limit(5).write.mode('overwrite').parquet(output_data+"immi")
    ni = immi.count()
    print("Immigration data uploaded, %s seconds" % (chrono.time() - start_time))
    
    demo.printSchema()
    start_time = chrono.time()
    #Change the selected columns below to just upload a sample.
    demo.write.mode('overwrite').parquet(output_data+'demo')
    #demo.limit(5).write.mode('overwrite').parquet(output_data+'demo')
    nd = demo.count()
    print("Demographics data uploaded, %s seconds" % (chrono.time() - start_time))
    
    time.printSchema()
    start_time = chrono.time()
    #Change the selected columns below to just upload a sample.
    time.write.mode('overwrite').parquet(output_data+'time')
    #time.limit(5).write.mode('overwrite').parquet(output_data+'time')
    nc = time.count()
    print("Time data uploaded, %s seconds" % (chrono.time() - start_time))
    
    airp.printSchema()
    start_time = chrono.time()
    #Change the selected columns below to just upload a sample.
    airp.write.mode('overwrite').parquet(output_data+'airp')
    #airp.limit(5).write.mode('overwrite').parquet(output_data+'airp')
    na = airp.count()
    print("Airport data uploaded, %s seconds" % (chrono.time() - start_time))

    print("Upload to s3 completed")
    
    n = [ni, nt, nd, na, nc]
    return n


# Step 3 Redshift

def create_database():
    """
    Drop old tables and create new ones.
    
    Params: None.
    """
    config = configparser.ConfigParser()
    config.read('config.cfg')
    
    conn = psycopg2.connect("user={} password={} host={} port={} dbname={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Dropping old tables")
    drop_tables(cur, conn)
    print("Old tables dropped")
    print("Creating new tables")
    create_tables(cur, conn)
    print("New tables created")

    conn.close()


def drop_tables(cur, conn):
    """
    Drop all the tables created if they exists.
    
    Params:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all the tables needed.
    
    Params:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def insert_redshift():
    """
    Load the information from S3, and copy it to the Redshift database.
    
    Params: None
    """

    config = configparser.ConfigParser()
    config.read('config.cfg')
    
    conn = psycopg2.connect("user={} password={} host={} port={} dbname={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    for query in copy_table_queries:
        loadTimes = []
        t0 = chrono.time()
        print("======= LOADING TABLE =======")
        
        cur.execute(query)
        conn.commit()

        loadTime = chrono.time()-t0
        loadTimes.append(loadTime)
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
        

# Step 4 Quality

def quality_database(n):
    """
    Function to compare the total number of rows inserted in the database with.
    
    Params: Array
    """

    config = configparser.ConfigParser()
    config.read('config.cfg')
    
    conn = psycopg2.connect("user={} password={} host={} port={} dbname={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    i=0
    for query in quality_table_queries:
        
        cur.execute(query)
        rows = cur.fetchall()
        
        for row in rows:
            m = n[i] - row [0]
            if m == 0:
                print("No rows missing")
            else:
                print(f"Number of rows missing: {m}")
        i+=1
        

def integrity_database():
    """
    Function to check the percentage of not null values in the key columns.
    
    Params: None.
    """
    config = configparser.ConfigParser()
    config.read('config.cfg')
    
    conn = psycopg2.connect("user={} password={} host={} port={} dbname={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    for query in integrity_table_queries:
        
        cur.execute(query)
        rows = cur.fetchone()
        
        print(rows[0])
        rows = rows[1:]
        rows = list(rows)
        
        
        for row in rows:
            m = row
            if m == 100:
                print("Perfect integrity")
            else:
                print(f"Percentage of completness: {m}")