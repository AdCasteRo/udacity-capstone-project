import pandas as pd
from pandas import melt
from pyspark.sql.functions import *
import re
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, immi_cast, temp_cast, airp_cast, demo_cast, quality_table_queries
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sqlalchemy import create_engine
import time as chrono



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
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business', '2': 'Pleasure', '3' : 'Student'}
    
    i94cit_res_df = pd.DataFrame(list(i94cit_res.items()), columns=['Code', 'Country'])       
    i94port_df = pd.DataFrame(list(i94port.items()), columns=['Code', 'Port'])   
    i94mode_df = pd.DataFrame(list(i94mode.items()), columns=['Code', 'Mode'])   
    i94addr_df = pd.DataFrame(list(i94addr.items()), columns=['Code', 'State'])   
    i94visa_df = pd.DataFrame(list(i94visa.items()), columns=['Code', 'Type'])   
    
    
    return i94cit_res_df, i94port_df, i94mode_df, i94addr_df, i94visa_df  


def drop_tables(cur, conn):
    """
    Drop all the tables created if they exists
    Keyword arguments:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all the tables needed
    Keyword arguments:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def create_database():
    """Drop old tables and create new ones"""
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

def spark_init():
    """
    Return a Spark Session with the adecuate parameters.
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



def create_time(spark, immi_df, temp_df):
    """
    Using Spark, gets the date information from the immigration table and temperature table and creates the time table.
    
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

def cast_dataframe(spark, immi_df, temp_df, demo_df, airp_df):
    
    i94cit_res, i94port, i94mode, i94addr, i94visa = labels_reader()
    i94cit_res = spark.createDataFrame(i94cit_res)
    i94port = spark.createDataFrame(i94port)
    i94mode = spark.createDataFrame(i94mode)
    i94addr = spark.createDataFrame(i94addr)
    i94visa = spark.createDataFrame(i94visa)
    i94cit_res.createOrReplaceTempView("cit_res")
    i94port.createOrReplaceTempView("port")
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


def upload_s3(spark, immi, temp, demo, airp, time):
    """
    Upload all the tables to S3.
    """
    
    output_data = 's3a://acr-udacity-capstone-bucket-2/'
    print("Starting upload to S3")
    
    temp.printSchema()
    start_time = chrono.time()
    temp.write.mode('overwrite').parquet(output_data+'temp')
    nt = temp.count()
    print("Temperature data uploaded, %s seconds" % (chrono.time() - start_time))
    
    immi.printSchema()
    start_time = chrono.time()
    immi.write.mode('overwrite').parquet(output_data+"immi")
    ni = immi.count()
    print("Immigration data uploaded, %s seconds" % (chrono.time() - start_time))
    
    demo.printSchema()
    start_time = chrono.time()
    demo.write.mode('overwrite').parquet(output_data+'demo')
    nd = demo.count()
    print("Demographics data uploaded, %s seconds" % (chrono.time() - start_time))
    
    time.printSchema()
    start_time = chrono.time()
    time.write.mode('overwrite').parquet(output_data+'time')
    nc = time.count()
    print("Time data uploaded, %s seconds" % (chrono.time() - start_time))
    
    airp.printSchema()
    start_time = chrono.time()
    airp.write.mode('overwrite').parquet(output_data+'airp')
    na = airp.count()
    print("Airport data uploaded, %s seconds" % (chrono.time() - start_time))

    print("Upload to s3 completed")
    
    n = [ni, nt, nd, na, nc]
    return n


def insert_redshift():
    """
    Load the information from S3, and copy it to the Redshift database
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
        

def quality_database(n):

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
