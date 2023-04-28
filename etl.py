# """ ETL Main File """
import configparser              # required for reading out config-file [*.cfg]
from datetime import datetime    # 
import os                        # required for setting environment variables, like IAM-Authentification

import numpy as np
import matplotlib.pyplot as plt  # Pyplot - Bar Graph, Line Graph, Others (pending)

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col   #, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import StructType  as R,   StructField as Fld, \
                              DoubleType  as Dbl, StringType  as Str, \
                              IntegerType as Int, DateType    as Date, \
                              BooleanType as Bol, DecimalType as Dec, \
                              FloatType   as Flt, TimestampType as TS, \
                              LongType    as Lng
                              
import pandas as pd



def create_spark_session():
    """HELP for procedure CREATE_SPARK_SESSION

    Creation of or retrieves Spark Session
    
    Parameters: 
    ( none )
    """
		
		os.environ["JAVA_HOME"]   = "/usr/lib/jvm/java-8-openjdk-amd64"
		os.environ["PATH"]        = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:" \
		                            "/opt/conda/bin:/usr/local/sbin:/usr/local/bin:" \
		                            "/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"

		os.environ["SPARK_HOME"]  = "/opt/spark-2.4.3-bin-hadoop2.7"
		os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

		config = configparser.ConfigParser()
		config.read('aws-access.cfg')
		print(">> Read Out Config-Infos from [aws-access.cfg]")

		os.environ['AWS_ACCESS_KEY_ID']     = config['AWS']['AWS_ACCESS_KEY_ID']
		os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
		
		AWS_ACCESS_KEY_ID     = config['AWS']['AWS_ACCESS_KEY_ID']
		AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

		spark = SparkSession.builder \
		                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
		                    .config("spark.executor.instances", 10) \
		                    .config("spark.executor.memory", "8g") \
		                    .enableHiveSupport() \
		                    .getOrCreate()

		sc    = spark.sparkContext
		sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
		sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

		# @ EMR (AWS): "emr.5.20.0" > "hadoop 2.7.0"
		# @ EMR (AWS): "emr-5.31.0" > "hadoop 2.10.0"

    return spark



def read_csvs(input_data)
    """HELP for procedure READ_CSVS
    
    Reads the CSV files needed for execution of project
    Does return them as pandas dataframes for later data preparation

    Parameters: 
    - input_data: Data Directory source path
    """
    
		# define (csv) filenames 
		nhoods_csv_fn   = "neighbourhoods.csv"
		list_csv_fn     = "listings.csv"
		list_det_csv_fn = "listings_detailed.csv"  # < listings.csv.gz
		calendar_csv_fn = "calendar.csv"           # < calendar.csv.gz
		#reviews_csv_fn  = "reviews.csv"
		rev_det_csv_fn  = "reviews_detailed.csv"   # < reviews.csv.gz
		
		hoods_csv   = input_data + nhoods_csv_fn
		list_csv     = input_data + list_csv_fn
		list_det_csv = input_data + list_det_csv_fn
		calendar_csv = input_data + calendar_csv_fn
		#reviews_csv  = input_data + reviews_csv_fn
		rev_det_csv  = input_data + rev_det_csv_fn

		print(">> Source path [" + nhoods_csv_fn   + "]: [" + hoods_csv   + "]")
		print(">> Source path [" + list_csv_fn     + "]: [" + list_csv     + "]")
		print(">> Source path [" + list_det_csv_fn + "]: [" + list_det_csv + "]")
		print(">> Source path [" + calendar_csv_fn + "]: [" + calendar_csv + "]")
		#print(">> Source path [" + reviews_csv_fn  + "]: [" + reviews_csv  + "]")
		print(">> Source path [" + rev_det_csv_fn  + "]: [" + rev_det_csv  + "]")		
    
    #read csv file contents into pandas dataframe
    hoods_df    = pd.read_csv(hoods_csv)
    list_df     = pd.read_csv(list_csv)
    list_det_df = pd.read_csv(list_det_csv)
    calendar_df = pd.read_csv(calendar_csv)
    reviews_df  = pd.read_csv(rev_det_csv)
    		
		return hoods_df, list_df, list_det_df, calendar_df, reviews_df, \
		       nhoods_csv, list_csv, list_det_csv, calendar_csv, rev_det_csv



def data_preparation(hoods_df, list_df, list_det_df, calendar_df, reviews_df)
    """HELP for procedure DATA_PREPARATION
    
    Makes data cleaning and preparation of identified steps
    See also single steps on jupyter notebook

    Parameters: 
    - Data Frames of different sources
    """
    
		calendar_df['adjusted_price'] = calendar_df['adjusted_price'].str.replace('$', '')

		list_df = list_df.fillna('')
    null_to_zero = ['reviews_per_month','accomodates','bathrooms','bedrooms','beds']
    for i in null_to_zero
        list_df[i] = list_df[i].replace('', 0)

    # Print the number of duplicate rows in each dataframe
    duplicated_list_id = list_df[list_df.duplicated(['id'])]['id'].count()
    print(Number of duplicate list_ids in list_df, duplicated_list_id)

    duplicated_review_id = reviews_df[reviews_df.duplicated(['id'])]['id'].count()
    print(Number of duplicate review_ids in review_df, duplicated_review_id)
		
		   

def data_types()
    """HELP for procedure DATA_TYPES
    
    Set wished data type to the dataframes before writing to S3.
    Mostly the dataSchemas are pre-defined for the datas.

    Parameters: 
    (none)
    """
    
    # define schema as data type is required
    hoodSchema = R([
		    Fld("neighbourhood_group", Str()),
		    Fld("neighbourhood",       Str()),
		])
    
		list_detSchema = R([
		    Fld("id",                     Int()),
		    Fld("name",                   Str()),
		    Fld("description",            Str()),
		    Fld("host_id",                Int()),
		    Fld("host_name",              Str()),
		    Fld("host_since",             Date()),
		    Fld("source",                 Str()),
		    Fld("latitude",               Flt()),
		    Fld("longitude",              Flt()),
		    Fld("price",                  Flt()),
		    Fld("review_scores_rating",   Int()),
		    Fld("reviews_per_month",      Flt()),
		    Fld("room_type",              Str()),
		    Fld("accommodates",           Int()),
		    Fld("bathrooms",              Int()),
		    Fld("bedrooms",               Int()),
		    Fld("beds",                   Int()),
		    Fld("neighbourhood_cleansed", Str()),
		])
    
    # Alredy here cutting (cleaning up) columns ['price','m*']!
    calendarSchema = R([
		    Fld("listing_id",     Int()),
		    Fld("date",           Date()),
		    Fld("available",      Str()),
		    Fld("adjusted_price", Str()),
		])
    
    rev_detSchema = R([
		    Fld("listing_id",    Int()),
		    Fld("id",            Int()),
		    Fld("date",          Date()),
		    Fld("reviewer_id",   Int()),
		    Fld("reviewer_name", Str()),
		    Fld("comments",      Str()),
		])

    print(">> DataFrame Schemas are declared")
    
    return hoodSchema, list_detSchema, calendarSchema, rev_detSchema
    
    

def pre_processing_s3(nhoods_csv, list_csv, list_det_csv, calendar_csv, rev_det_csv)
    """HELP for procedure PRE_PROCESSING_S3
    
    Creation of the Spark DataFrames as basis for upload to S3 AWS Bucket.
    Indirectly the tables bases.

    Parameters: 
    - The Names of sources
    """

		# read out and create spark dataframes ("_df"), NOT Data Feed ("df_")
		df_nhoods   = spark.read.csv(nhoods_csv, hoodSchema)
		
		df_list     = spark.read.option("header", "true").format("csv").schema(listSchema).load(list_csv)
		
		df_listing  = spark.read.option("header", "true").format("csv") \
		                   .schema(list_detSchema) \
		                   .load(list_det_csv)

		df_calendar = spark.read.option('header', 'true') \
		                   .option('sep', ',') \
		                   .format('csv') \
		                   .schema(calendarSchema) \
		                   .load(calendar_csv)
		                   
		#df_reviews  = spark.read.csv(reviews_csv)
		df_rev_det  = rev_det_df = spark.read.format("csv") \
		                   .option("header",    True) \
		                   .option('quotes',    '"') \
		                   .option("delimiter", ',') \
		                   .schema(rev_detSchema) \
		                   .load(rev_det_csv)
		                   
		
		    
    # Select columns for the various tables preparing upload to S3 as parquet
    # +++
    
    # NEIGHBOURHOODS - extract columns to create Hoods table
    hoods_table = df_hoods.select('neighbourhood_group', 'neighbourhood')

    # LISTING - extract columns to create LISTING table
    df_listing       = df_listing.withColumn('month', month('host_since'))
    # Only using columns of interest (demo). Including all will slow down execution for review
    listing_columns  = ['id','name','description','host_id','host_name','host_since', \
                       'month','source','latitude','longitude','price', \
                       'review_scores_rating','reviews_per_month','room_type', \
                       'accommodates','bathrooms','bedrooms','beds','neighbourhood_cleansed']
    listing_table    = df_listing.selectExpr(listing_columns).dropDuplicates()
    
    # CALENDAR - extract columns to create CALENDAR table
    df_calendar      = df_calendar.withColumn('month', month('date'))
    calendar_columns = ['listing_id', 'date', 'available', 'adjusted_price', 'month']
    calendar_table   = df_calendar.selectExpr(calendar_columns).dropDuplicates()

    # REVIEWS TABLE - extract columns to create REVIEWS table
    df_reviews = df_reviews.withColumnRenamed("id",   "review_id") \
                           .withColumnRenamed("date", "review_date")
    df_reviews       = df_reviews.na.drop()
    df_reviews       = df_reviews.withColumn('month', month('review_date'))
    reviews_columns  = ['listing_id', 'review_id', 'review_date', \
                        'reviewer_id', 'reviewer_name', 'comments', 'month']
    reviews_table    = df_reviews.selectExpr(reviews_columns).dropDuplicates()


    ## Creating the FACT table BOOKINGS now
    df_bookings_stg  = df_calendar.join(listing_table, \
                                        on = [calendar_table.listing_id == \
                                            listing_table.id], \
                                        how = 'left')
    df_bookings      = df_bookings_stg.join(reviews_table, \
                                        on = [df_bookings_stg.reviewer_id == \
                                              reviews_table.reviewer_id], \
                                        how = 'left')

    # preparing BOOKINGS before uploading to the S3 as parquet
    df_bookings      = df_bookings.withColumn('month', month('review_date'))
    # Only using columns of interest (demo). Including all will slow down execution for review
    booking_table    = df_bookings.select('id','name','description','host_id','host_name', \
                         'host_since','source','latitude','longitude','adjusted_price', \
                         'review_id','review_date','reviewer_id','review_scores_rating', \
                         'reviews_per_month','room_type','accommodates','bathrooms', \
                         'bedrooms','beds','month_review')
                                      
    print('>> Tables data is prepared and ready to be uploaded to S3')
		


def write_to_s3(output_bucket)
    """HELP for procedure WRITE_TO_S3
    
    Procedure does write Spark DataFrame to s3 as Parquet files

    Parameters: 
    - output_bucket: target directory where to save data on S3 at AWS
    """
    
    # WRITING TABLES AS PARQUET TO S3
    
    # write NEIGHBOURHOODS table to parquet files partitioned by neighbourhood_group
    hoods_table.write.mode('overwrite').partitionBy('neighbourhood_group').parquet(output_bucket + 'neighbourhoods')
    print('>> NEIGHBOURHOODS Table is created on the S3 bucket!')

    # write LISTING table to parquet files partitioned by month and room type
    listing_table.write.partitionBy('month', 'room_type') \
                 .parquet(output_bucket + 'listing', 'overwrite')
    print('>> LISTING Table is created on the S3 bucket!')

    # write CALENDAR table to parquet files partitioned by month
    calendar_table.write.partitionBy('month') \
                  .parquet(output_bucket + 'calendar', 'overwrite')
    print('>> CALENDAR Table is created on the S3 bucket!')

    # write REVIEW table to parquet files partitioned by month 
    reviews_table.write.partitionBy('month') \
                 .parquet(output_bucket + 'reviews', 'overwrite')
    print('>> REVIEW Table is created on the S3 bucket!')

    # write BOOKINGS (fact) table to parquet files partitioned month
    booking_table.write.partitionBy('month') \
                 .parquet(output_bucket + 'booking', 'overwrite')
    print('>> BOOKING Table is created on the S3 bucket!')
    
    

def data_quality_check(output_data)
    """HELP for procedure DATA_QUALITY_CHECK
    
    Reread from S3 the parquet files uploaded before.
    A check will be runs, to see if the the amount of up-/downloaded rows does match

    Parameters: 
    - output_data: online source directory of parquet files on S3 at AWS
    """

    # From S3 read tables in order to parse and do analysis ...
    neighbourhoods = spark.read.parquet(output_data + 'neighbourhoods')
    listing        = spark.read.parquet(output_data + 'listing')
    calendar       = spark.read.parquet(output_data + 'calendar')
    reviews        = spark.read.parquet(output_data + 'reviews')
    booking        = spark.read.option('mergeSchema', True).parquet(output_data + 'booking')
    
    # Get the count of rows in the dataframe
    uploaded_table    = ['booking_table','listing_table','reviews_table','hoods_table','calendar_table']
    downloaded_tables = ['booking','listing','reviews','neighbourhoods','calendar']

    expected_count = []
    row_count      = []

    for i in uploaded_table:
    	  expected_count.append(i)

    for i in downloaded_tables:
    	  row_count.append(i)

    # Compare the counts
    for uploaded, downloaded in zip(uploaded_table, downloaded_tables):
    	    if downloaded.count() != uploaded.count():
    	    	        raise ValueError(">> Data is incomplete. Expected {} rows in {} but found {}" \
    	    	                         .format(uploaded.count(), uploaded, downloaded.count()) \
    	    	                        )



def main():
    """HELP for ETL (main)

    Extract songs and events data from S3, transform it into dimensional tables
    format and uploads back into S3 in PARQUET format
    
    Subprocesses
    - create_spark_session
    - read_csvs
    - data_preparation
    - data_types
    - pre_processing_s3
    - write_to_s3
    - data_quality_check
    """
    
    print('>> START (main)')

    # (A) creating Spark Session 
    spark = create_spark_session()
    print('>> spark session created')

    # AWS Settings @ EMR Notebook Environment
    ##output_data = "s3a://capstone-project-out/"
    # + + + + + 
    # LOC Setting @ Udacity Workbook Environment
    input_data  = "./"   # "data/"
    output_data = "data_output/"
    
    # (B) Assign Names of CSV files
    [hoods_df, list_df, list_det_df, calendar_df, reviews_df, \
     nhoods_csv, list_csv, list_det_csv, calendar_csv, rev_det_csv] = read_csvs(input_data) 
    
    # (C) Prepare Data 
    data_preparation(hoods_df, list_df, list_det_df, calendar_df, reviews_df)
    
    # (D) Construct Data Schemas
    [hoodSchema, list_detSchema, calendarSchema, rev_detSchema] = data_types()
    
    # (E) Read and Pre-Process Data
    pre_processing_s3(nhoods_csv, list_csv, list_det_csv, calendar_csv, rev_det_csv)
    
    # (F) Write Data to S3 Bucket at AWS
    write_to_s3(output_bucket)
    
    # (G) Make some Checks
    data_quality_check(output_data)
    

    print('>> END! [etl.py] executed!')


if __name__ == "__main__":
    main()
 

# +++ EOF +++ [etl.py] +++
