from airflow import DAG
from datetime import datetime, timedelta,date
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable


## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_demog")
start_date = Variable.get("start_date",deserialize_json=True)


## Default Arguments ##
default_args = { 
    'owner': 'top_full_str_demog',
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
    'top_full_str_demog', catchup = False,default_args=default_args, schedule_interval=schedule_interval)


# BigQuery Target Store Check
Target_Store_Check = ExternalTaskSensor(
    task_id='Target_Store_Check',
    external_dag_id = 'top_full_mstr_str_merch',
    external_task_id = '1_4_Target_Store',
    #allowed_states=None,
    #execution_delta=timedelta(minutes=30),
    dag = dag)

# BigQuery Source to BigQuery Stage
Full_Load_Stage_Store_Demographics =  BigQueryOperator(
    task_id='Full_Load_Stage_Store_Demographics',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_str_demog/1_Stage_Store_Demographics.sql',
	params=dag_config,
	dag=dag)

# BigQuery Stage to BigQuery Target
Full_Load_Target_Store_Demographics =  BigQueryOperator(
    task_id='Full_Load_Target_Store_Demographics',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_str_demog/2_Target_Store_Demographics.sql',
	params=dag_config,
	dag=dag)

# BigQuery Target to Google Cloud Storage
Full_Load_GCS_Store_Demographics = BigQueryToCloudStorageOperator(
    task_id = 'Full_Load_GCS_Store_Demographics',
    source_project_dataset_table = "{{ var.json.ENV_Variables.Optumera_Target_DS }}"+".Store_Demographics",
    destination_cloud_storage_uris = "{{ var.json.ENV_Variables.Export_URI_FullLoad }}"+"/store_demographics/store_demographics.csv",
    export_format = 'CSV',
    field_delimiter = ',',
    replace = True,
    print_header = False,
    dag = dag
)

Full_Load_Stage_Store_Demographics >> Target_Store_Check >> Full_Load_Target_Store_Demographics >> Full_Load_GCS_Store_Demographics