import psycopg2
import boto3
import traceback
import json
import sys
from awsglue.utils import getResolvedOptions

print("****** Glue Job started ******")
    
 




# Transaction tables
# Representation - {"rmpa_prod_transform table" : ["corresponding rmpa_prod_staging table", "primary_key1", "primary_key2",...]}
transaction_table_dict = {"rmpa_prod_transform.sh_invoice_dedup"     : ["rmpa_prod_staging.sh_invoice", 
                                                               "PARENT_GROUP", 
                                                               "DEALER_MAP_CD", 
                                                               "LOC_CD", 
                                                               "COMP_FA", 
                                                               "inv_num",
                                                               "inv_type"], 
                          "rmpa_prod_transform.sh_ordbook_dedup" : ["rmpa_prod_staging.SH_ORDBOOK", 
                                                               "PARENT_GROUP", 
                                                               "DEALER_MAP_CD", 
                                                               "LOC_CD", 
                                                               "COMP_FA", 
                                                               "ORDER_NUM"]}





# Transaction tables related operations
def redshift_transaction_operations(transform_table_with_schema_name, list_items):
    """
    Deletes already existing rows from rmpa_prod_transform tables by referring rmpa_prod_staging table and loads new data.
    """
    staging_table_with_schema_name = list_items[0]
    primary_keys_list = list_items[1:]
    new_list = []
    staging_table_without_schema = staging_table_with_schema_name.split(".")[1]
    transform_table_without_schema = transform_table_with_schema_name.split(".")[1]

    for pk in primary_keys_list:
        new_list.append("{0}.{2} = {1}.{2}" \
                        .format(transform_table_without_schema, staging_table_without_schema, pk))

    where_condition = " AND ".join(new_list)

    sql1 = "BEGIN; \
            DELETE FROM {0} USING {1} WHERE {2}; \
            END;" \
           .format(transform_table_with_schema_name, staging_table_with_schema_name, where_condition)
    sql2 = "INSERT INTO {0} SELECT * FROM {1};" \
           .format(transform_table_with_schema_name, staging_table_with_schema_name)

    print(sql1)
    print(sql2)

    cur.execute(sql1)
    print("Delete completed")
    cur.execute(sql2)
    print("Inserted records in rmpa_prod_transform table")
    con.commit()
    print("{} completed".format(transform_table_with_schema_name))

def rural_main_function():
    """
    For each Master tables redshift_master_operations() function will be called. 
    Function parameters: rmpa_prod_transform table name, rmpa_prod_staging table name
    For each Transaction tables redshift_transaction_operations() function will be called.
    Function parameters: rmpa_prod_transform table name, a list that has rmpa_prod_staging table name and primary keys
    """
    

    # Transaction tables
    for transform_table_with_schema_name in transaction_table_dict:
        try:
            redshift_transaction_operations(transform_table_with_schema_name, transaction_table_dict[transform_table_with_schema_name])

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