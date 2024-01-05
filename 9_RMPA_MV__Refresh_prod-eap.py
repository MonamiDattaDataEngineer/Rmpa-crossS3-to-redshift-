import datetime, pytz
from datetime import timedelta
import pandas as pd
import boto3
import traceback
import json
import psycopg2
import materialized_view_config

    


try:
	con = psycopg2.connect(dbname='rmpa_db', port='5439' ,user='rmpa_user', password='rMp@1234', host='ace-common-cluster-1.csuh9tvc69yu.ap-south-1.redshift.amazonaws.com')
	cur = con.cursor()
	print("Redshift connnection created")
except Exception as e:
	print("Exception while creating connection to Redshift Cluster")
	print(e)
    
dict_oracle = materialized_view_config.table_list    
    
for table in dict_oracle:
    print("--------------------------------------")
    transform_schema = dict_oracle[table]['raw']['schema']
    iam_role = 'arn:aws:iam::264252882810:role/AWS-Redshift_role-RMPA'
    delim = ','
    reg_name = 'ap-south-1'  

    
    
    
    try:
        refresh_query = f'refresh materialized view {transform_schema}.{table};END;'
        
         
        print(refresh_query)
        cur.execute(refresh_query)
        con.commit()
        print(f'Materialized view succesfully refreshed {table}')
        
    
    except Exception as e:
        print(f'Exception while refreshing MV  {table}')
        print(e)
        con.commit()
        continue
        #sns_notification(project_name, resource_name, job_name, str(e))
        #raise



print("job completed")




 Location of config file0----   s3://rmpa-prod-rawdata/config_files/materialized_view_config.py
 
 
 Content of config file-------->>
 
 
 table_list ={ 
'Sh_invoice_mv' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

},

'rmpa_dms_detailed_final_village_coverage_logic_mv' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

},

'rmpa_dms_mmi_detailed_final_village_coverage_logic_mv' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

},

'rmpa_emr_outlet_mv' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

},

'rmpa_tehsil_rdse_count_mv' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

},

'state_report_summary' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

},

'region_report_summary' :
{ 
'raw' : { 'schema' : 'rmpa_prod_transform'}

}
}