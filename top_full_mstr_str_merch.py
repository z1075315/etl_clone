from airflow import DAG
from datetime import datetime, timedelta,date
from airflow.operators import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_full_load")
start_date = Variable.get("start_date",deserialize_json=True)


## Default Arguments ##
default_args = { 
    'owner': 'top_full_mstr_str_merch',
    'start_date': datetime.strptime(start_date, '%Y,%m,%d'),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Create the dag insatance
dag = DAG(
    'top_full_mstr_str_merch', catchup=False,default_args=default_args, schedule_interval=schedule_interval)

   
#airflow.apache.org/_modules/airflow/contrib/operators/bigquery_to_gcs.html
'''
Clean_Bucket = BashOperator(
    task_id='Clean_Bucket',
    bash_command='gsutil -m rm gs://etl_data_extract/full_load/**',
    dag=dag,
)'''

Build_Stage_Store =  BigQueryOperator(
    task_id='1_3_Stage_Store',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_mstr_str_merch/1_3_Stage_Store.sql',
	params=dag_config,
	dag=dag)

Build_Target_Store =  BigQueryOperator(
    task_id='1_4_Target_Store',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_mstr_str_merch/1_4_Target_Store.sql',
	params=dag_config,
	dag=dag)

Export_Store = BigQueryToCloudStorageOperator(
    task_id = 'export_store',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Store",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_FullLoad}}"+"/store/store.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)

Merch_Hierarchy =  BigQueryOperator(
	task_id='2_1_Merch_Hierarchy',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_full_mstr_str_merch/2_1_Merch_Hierarchy.sql',
	params=dag_config,
	dag=dag)

Stage_Merch_Hierarchy =  BigQueryOperator(
	task_id='2_2_Stage_Merch_Hierarchy',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_full_mstr_str_merch/2_2_Stage_Merch_Hierarchy.sql',
	params=dag_config,
	dag=dag)


Build_Merch_Level1 =  BigQueryOperator(
	task_id='2_3_Merch_Level1',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_full_mstr_str_merch/2_3_Merch_Level1.sql',
	params=dag_config,
	dag=dag)

Build_Merch_Level2 =  BigQueryOperator(
	task_id='2_4_Merch_Level2',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_full_mstr_str_merch/2_4_Merch_Level2.sql',
	params=dag_config,
	dag=dag)
	
Build_Merch_Level3 =  BigQueryOperator(
	task_id='2_5_Merch_Level3',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_full_mstr_str_merch/2_5_Merch_Level3.sql',
	params=dag_config,
	dag=dag)

Export_Merch_Level1 = BigQueryToCloudStorageOperator(
    task_id = 'export_merch_level1',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Merch_Level1",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_FullLoad}}"+"/Merch_Level1/Merch_Level1.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)
Export_Merch_Level2 = BigQueryToCloudStorageOperator(
    task_id = 'export_merch_level2',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Merch_Level2",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_FullLoad}}"+"/Merch_Level2/Merch_Level2.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)
Export_Merch_Level3 = BigQueryToCloudStorageOperator(
    task_id = 'export_merch_level3',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Merch_Level3",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_FullLoad}}"+"/Merch_Level3/Merch_Level3.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)	

success = DummyOperator(
    task_id='success',
    dag=dag
)
Build_Stage_Store >> Build_Target_Store >> Export_Store >> success
Merch_Hierarchy >> Stage_Merch_Hierarchy >> Build_Merch_Level1 >> Build_Merch_Level2 >> Build_Merch_Level3
Build_Merch_Level1 >> Export_Merch_Level1 >> success
Build_Merch_Level2 >> Export_Merch_Level2 >> success
Build_Merch_Level3 >> Export_Merch_Level3 >> success