import datetime, pytz
from datetime import timedelta
import pandas as pd
import boto3
import traceback
import json
import psycopg2
import rmpa_prod_config

try:
	con = psycopg2.connect(dbname='rmpa_db', port='5439' ,user='rmpa_user', password='rMp@1234', host='ace-common-cluster-1.csuh9tvc69yu.ap-south-1.redshift.amazonaws.com')
	cur = con.cursor()
	print("Redshift connnection created")
except Exception as e:
	print("Exception while creating connection to Redshift Cluster")
	print(e)
    
dict_oracle = rmpa_prod_config.table_list    
    
for table in dict_oracle:
    print("--------------------------------------")
    project_bucket = dict_oracle[table]['raw']['raw_bucket']
    staging_schema = dict_oracle[table]['redshift']['staging']
    iam_role_1 = 'arn:aws:iam::825589354750:role/redshift_role1'
    iam_role_2 = 'arn:aws:iam::264252882810:role/AWS-Glue-role-RMPA'
    
    delim = ','
    reg_name = 'ap-south-1'
    tabletype = dict_oracle[table]['tabletype']   
    project_path = "s3://" + str(project_bucket) + "/" + table + "/"  
    #s3://rmpa-PROD-rawdata/am_dealer_loc/
    
    
    
    try:
        trunc_staging_query = f'TRUNCATE TABLE {staging_schema}.{table};END;'
        copy_command = f''' COPY {staging_schema}.{table} 
                            FROM '{project_path}' 
                            IAM_ROLE '{iam_role_1},{iam_role_2}'
                            FORMAT AS PARQUET;END;'''
        if tabletype == 'transaction': 
            print(project_path)
            cur.execute(trunc_staging_query)
            cur.execute(copy_command)
            con.commit()
            print(f'Transaction Data inserted into PROD Stage for {table}')
        
    
    except Exception as e:
        print(f'Exception while Ingesting data to PROD for table {table}')
        print(e)
        con.commit()
        continue
        #sns_notification(project_name, resource_name, job_name, str(e))
        #raise



print("job completed")




    