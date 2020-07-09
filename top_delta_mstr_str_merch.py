from airflow import DAG
from datetime import datetime, timedelta,date
from airflow.operators import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable

## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_delta")
start_date = Variable.get("start_date",deserialize_json=True)

## Default Arguments ##
default_args = { 
    'owner': 'top_delta_mstr_str_merch',
    'start_date': datetime.strptime(start_date, '%Y,%m,%d'),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Create the dag instance
dag = DAG(
    'top_delta_mstr_str_merch', catchup=False,default_args=default_args, schedule_interval=schedule_interval)

def check(ds, **kwargs):
    a = date.today()
    #a = datetime.strptime("2020,4,5", '%Y,%m,%d').date()
    b = a.month
    c = a.day
    if a.month in [1,4,7,10] and a.day in [1,2,3,4,5,6,7]:
        return "Delta_Stage_Store_Demographics"
    else:
        return "Without_Store_Demographics"

# Cleans the Bucket before each run  
Clean_Bucket = BashOperator(
    task_id='Clean_Bucket',
    bash_command='gsutil -m rm '+dag_config['Export_URI_DeltaLoad']+'/**',
    dag=dag,
)

Stage_Store_Prev =  BigQueryOperator(
	task_id='1_1_Stage_Store_Prev',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/1_1_Stage_Store_Prev.sql',
	params=dag_config,
	dag=dag)


Build_Stage_Store =  BigQueryOperator(
    task_id='1_4_Stage_Store',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_delta_mstr_str_merch/1_4_Stage_Store.sql',
	params=dag_config,
	dag=dag)

Build_Delta_Store =  BigQueryOperator(
    task_id='1_5_Delta_Store',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_delta_mstr_str_merch/1_5_Delta_Store.sql',
	params=dag_config,
	dag=dag)


Build_Target_Store =  BigQueryOperator(
    task_id='1_6_Target_Store',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_delta_mstr_str_merch/1_6_Target_Store.sql',
	params=dag_config,
	dag=dag)

Build_Target_Delta_Store =  BigQueryOperator(
    task_id='1_7_Target_Delta_Store',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_delta_mstr_str_merch/1_7_Target_Delta_Store.sql',
	params=dag_config,
	dag=dag)

Export_Store = BigQueryToCloudStorageOperator(
    task_id = 'export_store',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Delta_Store",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_DeltaLoad}}"+"/store/store.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)

Stage_Merch_Hierarchy_Prev =  BigQueryOperator(
	task_id='2_1_Stage_Merch_Hierarchy_Prev',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_1_Stage_Merch_Hierarchy_Prev.sql',
	params=dag_config,
	dag=dag)

Merch_Hierarchy =  BigQueryOperator(
	task_id='2_2_Merch_Hierarchy',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_2_Merch_Hierarchy.sql',
	params=dag_config,
	dag=dag)


Stage_Merch_Hierarchy =  BigQueryOperator(
	task_id='2_3_Stage_Merch_Hierarchy',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_3_Stage_Merch_Hierarchy.sql',
	params=dag_config,
	dag=dag)

Delta_Merch_Hierarchy_Department =  BigQueryOperator(
	task_id='2_4_Delta_Merch_Hierarchy_Department',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_4_Delta_Merch_Hierarchy_Department.sql',
	params=dag_config,
	dag=dag)
	
Delta_Merch_Hierarchy_SubDepartment =  BigQueryOperator(
	task_id='2_5_Delta_Merch_Hierarchy_SubDepartment',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_5_Delta_Merch_Hierarchy_SubDepartment.sql',
	params=dag_config,
	dag=dag)

Delta_Merch_Hierarchy_Category =  BigQueryOperator(
	task_id='2_6_Delta_Merch_Hierarchy_Category',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_6_Delta_Merch_Hierarchy_Category.sql',
	params=dag_config,
	dag=dag)
	
Target_Merch_Level1 =  BigQueryOperator(
	task_id='2_7_Target_Merch_Level1',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_7_Target_Merch_Level1.sql',
	params=dag_config,
	dag=dag)


Target_Merch_Level2 =  BigQueryOperator(
	task_id='2_8_Target_Merch_Level2',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_8_Target_Merch_Level2.sql',
	params=dag_config,
	dag=dag)
		

Target_Merch_Level3 =  BigQueryOperator(
	task_id='2_9_Target_Merch_Level3',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_9_Target_Merch_Level3.sql',
	params=dag_config,
	dag=dag)

Build_Delta_Merch_Level1 =  BigQueryOperator(
	task_id='2_10_Target_Delta_Merch_Level1',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_10_Target_Delta_Merch_Level1.sql',
	params=dag_config,
	dag=dag)
	
Build_Delta_Merch_Level2 =  BigQueryOperator(
	task_id='2_11_Target_Delta_Merch_Level2',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_11_Target_Delta_Merch_Level2.sql',
	params=dag_config,
	dag=dag)
	
Build_Delta_Merch_Level3 =  BigQueryOperator(
	task_id='2_12_Target_Delta_Merch_Level3',
	use_legacy_sql=False,
	allow_large_results=True,
	write_disposition='WRITE_TRUNCATE',
	sql='/sql/top_delta_mstr_str_merch/2_12_Target_Delta_Merch_Level3.sql',
	params=dag_config,
	dag=dag)

Export_Merch_Level1 = BigQueryToCloudStorageOperator(
    task_id = 'export_merch_level1',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Delta_Merch_Level1",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_DeltaLoad}}"+"/Merch_Level1/Merch_Level1.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)
Export_Merch_Level2 = BigQueryToCloudStorageOperator(
    task_id = 'export_merch_level2',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Delta_Merch_Level2",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_DeltaLoad}}"+"/Merch_Level2/Merch_Level2.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)
Export_Merch_Level3 = BigQueryToCloudStorageOperator(
    task_id = 'export_merch_level3',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Delta_Merch_Level3",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_DeltaLoad}}"+"/Merch_Level3/Merch_Level3.csv",
    export_format = 'CSV',
    field_delimiter = ',',
	replace = True,
    print_header = False,
    dag = dag
)	

# BigQuery Source to BigQuery Stage
Delta_Stage_Store_Demographics =  BigQueryOperator(
    task_id='Delta_Stage_Store_Demographics',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_str_demog/1_Stage_Store_Demographics.sql',
	params=dag_config,
	dag=dag)

# BigQuery Stage to BigQuery Target
Delta_Target_Store_Demographics =  BigQueryOperator(
    task_id='Delta_Target_Store_Demographics',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_str_demog/2_Target_Store_Demographics.sql',
	params=dag_config,
	dag=dag)

# BigQuery Target to Google Cloud Storage
Delta_GCS_Store_Demographics = BigQueryToCloudStorageOperator(
    task_id = 'Delta_GCS_Store_Demographics',
    source_project_dataset_table = "{{ var.json.ENV_Variables.Optumera_Target_DS }}"+".Store_Demographics",
    destination_cloud_storage_uris = "{{ var.json.ENV_Variables.Export_URI_DeltaLoad }}"+"/store_demographics/store_demographics.csv",
    export_format = 'CSV',
    field_delimiter = ',',
    replace = True,
    print_header = False,
    dag = dag
)

Quarterly_Check = BranchPythonOperator(
    task_id='Quarterly_Check',
    provide_context=True,
    python_callable=check,
    trigger_rule="all_done",
    dag=dag,
)

Without_Store_Demographics = BashOperator(
    task_id='Without_Store_Demographics',
    bash_command='touch store_demographics.csv;gsutil mv store_demographics.csv '+dag_config['Export_URI_DeltaLoad']+'/store_demographics/store_demographics.csv',
    dag=dag,
)

success = DummyOperator(
    task_id='success',
    trigger_rule="none_failed",
    dag=dag
)
Clean_Bucket >> Stage_Store_Prev >> Build_Stage_Store >> Build_Delta_Store >> Build_Target_Store
Build_Target_Store >> Build_Target_Delta_Store >>Export_Store >> Quarterly_Check
Quarterly_Check >> [Delta_Stage_Store_Demographics,Without_Store_Demographics]
Delta_Stage_Store_Demographics >> Delta_Target_Store_Demographics >> Delta_GCS_Store_Demographics >> success
Without_Store_Demographics >> success

Clean_Bucket >> Stage_Merch_Hierarchy_Prev >> Merch_Hierarchy >> Stage_Merch_Hierarchy >> Delta_Merch_Hierarchy_Department
Stage_Merch_Hierarchy >> Delta_Merch_Hierarchy_SubDepartment 
Stage_Merch_Hierarchy >> Delta_Merch_Hierarchy_Category
[Delta_Merch_Hierarchy_Department,Delta_Merch_Hierarchy_SubDepartment,Delta_Merch_Hierarchy_Category] >> Target_Merch_Level1 >> Build_Delta_Merch_Level1 >> Export_Merch_Level1 >>success
Target_Merch_Level1 >> Target_Merch_Level2 >> Build_Delta_Merch_Level2 >> Export_Merch_Level2 >>success
Target_Merch_Level2 >> Target_Merch_Level3 >> Build_Delta_Merch_Level3 >> Export_Merch_Level3 >>success
