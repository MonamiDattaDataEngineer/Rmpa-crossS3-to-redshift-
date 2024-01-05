import psycopg2
import boto3
import traceback
import json
import sys
from awsglue.utils import getResolvedOptions

print("****** Glue Job started ******")
    
# Master tables
# Representation - {"rmpa_prod_transform table" : "corresponding rmpa_prod_staging table"}
master_table_dict = {"rmpa_prod_transform.am_dealer_loc_dedup"     : "rmpa_prod_staging.am_dealer_loc", 
                     "rmpa_prod_transform.gm_regn_dedup"    : "rmpa_prod_staging.gm_regn", 
                     "rmpa_prod_transform.gm_state_dedup"   : "rmpa_prod_staging.gm_state", 
                     "rmpa_prod_transform.gm_tehsil_dedup"  : "rmpa_prod_staging.gm_tehsil", 
                     "rmpa_prod_transform.gm_village_dedup" : "rmpa_prod_staging.gm_village",
                     "rmpa_prod_transform.gm_emp_dedup" : "rmpa_prod_staging.gm_emp"
                     }    








# Master tables related operations
def redshift_master_operations(transform_table_with_schema_name, staging_table_with_schema_name):
    """
    Truncates and inserts all rows from rmpa_prod_staging table to rmpa_prod_transform table.
    """
    sql1 = "TRUNCATE TABLE {};" \
           .format(transform_table_with_schema_name)
    sql2 = "INSERT INTO {0} SELECT * FROM {1};" \
           .format(transform_table_with_schema_name, staging_table_with_schema_name)

    print(sql1)
    print(sql2)

    cur.execute(sql1)
    print("Truncated table")
    cur.execute(sql2)
    print("Inserted records in rmpa_prod_transform table")
    con.commit()
    print("{} completed.".format(transform_table_with_schema_name))



def rural_main_function():
    """
    For each Master tables redshift_master_operations() function will be called. 
    Function parameters: rmpa_prod_transform table name, rmpa_prod_staging table name
    For each Transaction tables redshift_transaction_operations() function will be called.
    Function parameters: rmpa_prod_transform table name, a list that has rmpa_prod_staging table name and primary keys
    """
    # Master tables
    for transform_table_with_schema_name in master_table_dict:
        try:
            redshift_master_operations(transform_table_with_schema_name, master_table_dict[transform_table_with_schema_name])

        # Exception block will call publish_failed_message() function and sends alert e-mail
        except Exception as e:
            print("Exception occured for {0} - {1}".format(transform_table_with_schema_name, e))
            status = 'failed'
            error_traceback = traceback.format_exc()
            publish_failed_message(error_traceback, status)

   

# Calls rural_main_function()
try:
    con = psycopg2.connect(dbname='rmpa_db', port='5439' ,user='rmpa_user', password='rMp@1234', host='ace-common-cluster-1.csuh9tvc69yu.ap-south-1.redshift.amazonaws.com')
    cur = con.cursor()
    print("Connected to Redshift cluster")
    rural_main_function()
    con.close()
except Exception as e:
    print("Exception while connecting to Redshift cluster.", e)
finally:
    print("msil_rural_s3_redshift_tgt_load completed.")
    print("****** Glue Job completed ******")