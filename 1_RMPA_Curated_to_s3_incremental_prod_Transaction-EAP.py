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
import traceback
#######Creating dynamodb and s3 client.
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
s3 = boto3.client('s3')

def publish_failed_message(error_traceback, status):
    """
    Function to publish alert e-mail to SNS if there are any exceptions.
    """
    try:
        print(status + " - " + error_traceback)
        subject = "RMPA_Curated_to_s3_Transaction_table"
        message = "This job {0} - {1}.".format(status, error_traceback)
        topic_arn = args['arn:aws:sns:ap-south-1:264252882810:RMPA_Alerts']
        sns = boto3.client("sns")
        sns.publish(TopicArn = topic_arn, Message = message, Subject = subject)
        print("Alert e-mail sent")
    except Exception as e:
            print("Exception occured while publishing message to SNS: ", e)
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
    
    # deltaTable = DeltaTable.forPath(spark, 's3://arn:aws:s3:us-east-1:334995291418:accesspoint/test-s3/data/sample_delta_table/')
    # df = deltaTable.toDF()
    #df = spark.read.format("delta").load('s3://test-s3-fo3xzkmpytkijdcxf6ccwh5docduyuse1a-s3alias/data/sample_delta_table/')
    #df = spark.read.parquet('s3://test-s3-fo3xzkmpytkijdcxf6ccwh5docduyuse1a-s3alias/data/realtimeparquet-verified/part-00000-02f34b16-34c0-417c-9cb2-9193b29233c9-c000.snappy.parquet')
    # Deltatable_gm_emp = spark.read.format("delta").load('s3://msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/gm_emp/')
    # Deltatable_gm_emp.show(5)
    # df_gm_emp = Deltatable_gm_emp.toDF()
    # df_gm_emp_pan = df_gm_emp.toPandas()
    # s3_gm_emp_file_path = "muldms/pd_issue/gm_emp/gm_emp.csv"
    # final_csv_str = df_gm_emp_pan.to_csv(None, sep='|', encoding='utf-8', header=True, index=False, quotechar='`', quoting=csv.QUOTE_ALL)

    # bucket = 'rmp-dev-rawdata'
    # s3 = boto3.resource('s3')
    # s3object = s3.Object(bucket, s3_gm_emp_file_path)
    # s3object.put(Body=(bytes(final_csv_str.encode('utf-8'))))

    
except Exception as e:
    print('Error : ' + str(e))




try:

    Deltatable_sh_invoice = DeltaTable.forPath(spark, "s3://msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/sh_invoice/")
    lastOperationDF_sh_invoice = Deltatable_sh_invoice.history(1) 
    version_id_sh_invoice = lastOperationDF_sh_invoice.select("version").collect()[0][0] 
    
    print(version_id_sh_invoice)
    
    df_sh_invoice = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", version_id_sh_invoice).load("s3://msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/sh_invoice/") 
        
    
    df_sh_invoice = df_sh_invoice.where((df_sh_invoice._change_type != 'update_preimage')).drop('_change_type','_commit_version','_commit_timestamp')
    
    print("sh_invoice Schema", df_sh_invoice.printSchema())
    df_sh_invoice = df_sh_invoice.select("dealer_map_cd","loc_cd","comp_fa","parent_group","inv_type","inv_num","inv_date","order_num","order_date","order_party_cd","allot_num","inv_party_cd","area_cd","variant_cd","ecolor_cd","icolor_cd","vin","engine_num","chassis_num","price_for_cd","desg_cd","cancel_date")
    print("sh_invoice Schema", df_sh_invoice.printSchema())
    print("sh_invoice count", df_sh_invoice.count())
    
    df_sh_invoice.write\
    .option("header" , True)\
    .mode("overwrite")\
    .parquet("s3://rmpa-prod-rawdata/sh_invoice/")
    
    Deltatable_sh_ordbook = DeltaTable.forPath(spark, "s3://msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/sh_ordbook/")
    lastOperationDF_sh_ordbook = Deltatable_sh_ordbook.history(1) 
    version_id_sh_ordbook = lastOperationDF_sh_ordbook.select("version").collect()[0][0] 
    
    print(version_id_sh_ordbook)
    
    df_sh_ordbook = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", version_id_sh_ordbook).load("s3://msil-cross-account-a-ex61r89zztim78oata5fe9bmmbanraps3b-s3alias/dms/muldms/sh_ordbook/") 
        
    
    df_sh_ordbook = df_sh_ordbook.where((df_sh_ordbook._change_type != 'update_preimage')).drop('_change_type','_commit_version','_commit_timestamp')
    print("sh_ordbook Schema", df_sh_ordbook.printSchema())
    df_sh_ordbook = df_sh_ordbook.select("dealer_map_cd","order_num","loc_cd","comp_fa","parent_group","state_cd","tehsil_cd","village_cd","village_type")
    print("sh_ordbook Schema", df_sh_ordbook.printSchema())
    print("sh_ordbook count", df_sh_ordbook.count())
    
    df_sh_ordbook.write\
    .option("header" , True)\
    .mode("overwrite")\
    .parquet("s3://rmpa-prod-rawdata/sh_ordbook/")
    
except Exception as e:
    print('Error : ' + str(e))
    status = 'failed'
    error_traceback = traceback.format_exc()
    publish_failed_message(error_traceback, status)   
    
    



