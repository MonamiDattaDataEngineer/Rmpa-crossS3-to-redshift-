import json
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from delta.tables import *
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, tzinfo
from hashlib import blake2b
import random
from operator import attrgetter
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from datetime import datetime, timedelta, date
from dateutil import rrule, parser
from calendar import monthrange 
import re
import rmpa_prod_config

#######Creating dynamodb and s3 client.
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
s3 = boto3.client('s3')
try:
    print('******************Glue Job started******************')
    print('******************Initializing spark session******************')
    spark = SparkSession.builder.appName("curation-layer") \
        .config("spark.sql.extensions", \
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",\
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled",\
                "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled",\
                "true") \
        .config("spark.databricks.delta.autoCompact.enabled",\
                "true") \
        .config("spark.databricks.delta.optimize.maxFileSize",\
                "104857600") \
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed",\
                "true")\
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    print('Spark session created. Session is available with name spark.')
    
    
except Exception as e:
    print('Error : ' + str(e))   

dict_oracle = rmpa_prod_config.table_list    
    
for table in dict_oracle:
    print("--------------------------------------")
    project_bucket = dict_oracle[table]['raw']['raw_bucket']
    cross_account = "msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/"
    source_path = "s3://" + cross_account + table + "/"
    print(source_path)
    #"s3://msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/sh_ordbook/"
    tabletype = dict_oracle[table]['tabletype']   
    project_path = "s3://" + str(project_bucket) + "/" + table + "/"  
    print(project_path)
    #s3://rmpa-uat-rawdata/am_dealer_loc/
    
    
    
    try:
                           
        if tabletype == 'master':
            Deltatable = DeltaTable.forPath(spark, source_path)
            df = Deltatable.toDF() 
            df.write\
            .option("header" , True)\
            .mode("overwrite")\
            .parquet(project_path)
            print(f'Master Data inserted into s3 for {table}')
    
    except Exception as e:
        print(f'Exception while Ingesting data to s3 for table {table}')
        print(e)
        con.commit()
        continue
        #sns_notification(project_name, resource_name, job_name, str(e))
        #raise



print("job completed")