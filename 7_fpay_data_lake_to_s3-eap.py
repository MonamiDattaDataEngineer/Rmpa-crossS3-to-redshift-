from pyspark.context import SparkContext
from awsglue.context import GlueContext
import rmpa_config_prod_3tables


# sc = SparkContext()
# glueContext = GlueContext(sc)

sc = SparkContext()
# Get current sparkconf which is set by glue
conf = sc.getConf()
# add additional spark configurations
conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
# Restart spark context
sc.stop()
sc = SparkContext.getOrCreate(conf=conf)
# create glue context with the restarted sc
glueContext = GlueContext(sc)

dict_oracle = rmpa_config_prod_3tables.table_list    
    
for table in dict_oracle:
    print("--------------------------------------")
    project_bucket = dict_oracle[table]['raw']['raw_bucket']
    #staging_schema = dict_oracle[table]['redshift']['staging']
    database = dict_oracle[table]['raw']['database']
    iam_role = 'arn:aws:iam::264252882810:role/AWS-Redshift_role-RMPA'
    delim = ','
    reg_name = 'ap-south-1'
    tabletype = dict_oracle[table]['tabletype']   
    project_path = "s3://" + str(project_bucket) + "/" + table + "/"  
    
    try:
        if tabletype == 'transaction':
            df = glueContext.create_data_frame.from_catalog(database=database, table_name=table)
            df.printSchema()
            if table == 'muldms_gm_desg':
                df = df.select("principal_map_cd","desg_cd","desg_desc","created_date","created_by","modified_date","modified_by","desig_group","enq_assign_yn","srv_enq_assign_yn","dealer_category","profile_id","profile_desc","profile_group")
                df.printSchema()
            else:
                df.write\
                .option("header" , True)\
                .mode("overwrite")\
                .parquet(project_path)
                print(f'Transaction Data inserted into s3 for {table}')
                
            df.write\
            .option("header" , True)\
            .mode("overwrite")\
            .parquet(project_path)
            print(f'Transaction Data inserted into s3 for {table}')
    
    except Exception as e:
        print(f'Exception while Ingesting data to s3 for table {table}')
        print(e)
        # con.commit()
        # continue
        #sns_notification(project_name, resource_name, job_name, str(e))
        #raise