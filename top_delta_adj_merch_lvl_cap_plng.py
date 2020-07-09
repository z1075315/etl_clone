"""
## DAG - ADJACENCY, CAPACITY AND PLANOGRAM ##

"""

## Package Imports ##
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta,date
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_delta")
start_date = Variable.get("start_date",deserialize_json=True)

## Default Arguments ##
default_args = { 
    'owner': 'top_delta_adj_merch_lvl_cap_plng',
    'start_date': datetime.strptime(start_date, '%Y,%m,%d'),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

## Create DAG instance ##
dag = DAG(
    'top_delta_adj_merch_lvl_cap_plng', catchup = False, default_args=default_args, schedule_interval=schedule_interval)
dag.doc_md = __doc__

## Task IDs ##
Adjacency_bigquery_task_ids = ['1_1_Adjacency_Footage_Live','1_2_Stage_Adjacency','1_3_Target_Str_Merch_Lvl3_Adjacency']
Capacity_bigquery_task_ids = ['2_1_Stage_Space_Level_Capacity','2_2_Target_Merch_Level_Capacity']
Planogram_bigquery_task_ids = ['3_1_Stage_Planogram','3_2_Target_Str_Merch_Lvl1_Planogram','3_3_Target_Str_Merch_Lvl2_Planogram','3_4_Target_Str_Merch_Lvl3_Planogram']


## Data_Load Function ##
def create_bq_task(task_name):
    return BigQueryOperator(
    task_id=task_name,
    write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_full_adj_merch_lvl_cap_plng/{}.sql'.format(task_name),
    params=dag_config,
    dag=dag)

## Dependency Table Sensor Check ##
def create_sensor_task(task_name,external_dag_id,external_task_id):
    return ExternalTaskSensor(task_id='sensor'+task_name, 
    external_dag_id = external_dag_id, 
    external_task_id = external_task_id, 
    dag=dag)

## BigQuery to GCS write_CSV ##
def create_bq_to_gcs_task(task_name,source_table_name,folder,file_name): 
    return BigQueryToCloudStorageOperator(
    task_id = task_name,
    source_project_dataset_table = "{{ var.json.ENV_Variables.Optumera_Target_DS }}."+source_table_name,
    destination_cloud_storage_uris = "{{ var.json.ENV_Variables.Export_URI_DeltaLoad }}/"+folder+"/"+file_name,
    export_format = 'CSV',
    field_delimiter = ',',
    replace = True,
    print_header = False,
    dag = dag)

## External Task Sensors ##
Sensor_Store = create_sensor_task('Store','top_delta_mstr_str_merch','1_6_Target_Store')
Sensor_Merch_Level1 = create_sensor_task('Merch_Level1','top_delta_mstr_str_merch','2_7_Target_Merch_Level1')
Sensor_Merch_Level2 = create_sensor_task('Merch_Level2','top_delta_mstr_str_merch','2_8_Target_Merch_Level2')
Sensor_Merch_Level3 = create_sensor_task('Merch_Level3','top_delta_mstr_str_merch','2_9_Target_Merch_Level3')


## ADJACENCY ##
# Tasks 
Bq_1_1_Adjacency_Footage_Live = create_bq_task('1_1_Adjacency_Footage_Live')
Bq_1_2_Stage_Adjacency = create_bq_task('1_2_Stage_Adjacency')
Bq_1_3_Target_Str_Merch_Lvl3_Adjacency = create_bq_task('1_3_Target_Str_Merch_Lvl3_Adjacency')

Bq_to_GCS_1_GCS_Target_Str_Merch_Lvl3_Adjacency = create_bq_to_gcs_task('1_Export_Str_Merch_Lvl3_Adjacency','Str_Merch_Lvl3_Adjacency','Str_Merch_Lvl3_Adjacency','Str_Merch_Lvl3_Adjacency.csv')

# Task Flow Dependencies 
Bq_1_1_Adjacency_Footage_Live >> Bq_1_2_Stage_Adjacency 

Sensor_Store << Bq_1_2_Stage_Adjacency
Sensor_Store >> Sensor_Merch_Level3

Sensor_Merch_Level3 >> Bq_1_3_Target_Str_Merch_Lvl3_Adjacency >> Bq_to_GCS_1_GCS_Target_Str_Merch_Lvl3_Adjacency


## CAPACITY ##
# Tasks 
Bq_2_1_Stage_Space_Level_Capacity = create_bq_task('2_1_Stage_Space_Level_Capacity')
Bq_2_2_Target_Merch_Level_Capacity = create_bq_task('2_2_Target_Merch_Level_Capacity')

Bq_to_GCS_2_GCS_Target_Merch_Level_Capacity = create_bq_to_gcs_task('2_Export_Merch_Level_Capacity','Merch_Level_Capacity','Merch_Level_Capacity','Merch_Level_Capacity.csv')

# Task Flow Dependencies  
Sensor_Store << Bq_2_1_Stage_Space_Level_Capacity
#Sensor_Store >> Sensor_Merch_Level3

Sensor_Merch_Level3 >> Bq_2_2_Target_Merch_Level_Capacity >> Bq_to_GCS_2_GCS_Target_Merch_Level_Capacity


## PLANOGRAM ##
# Tasks 
Bq_3_1_Stage_Planogram = create_bq_task('3_1_Stage_Planogram')
Bq_3_2_Target_Str_Merch_Lvl1_Planogram = create_bq_task('3_2_Target_Str_Merch_Lvl1_Planogram')
Bq_3_3_Target_Str_Merch_Lvl2_Planogram = create_bq_task('3_3_Target_Str_Merch_Lvl2_Planogram')
Bq_3_4_Target_Str_Merch_Lvl3_Planogram = create_bq_task('3_4_Target_Str_Merch_Lvl3_Planogram')

Bq_to_GCS_3_Target_Str_Merch_Lvl1_Planogram = create_bq_to_gcs_task('3_Export_Str_Merch_Lvl1_Planogram','Str_Merch_Lvl1_Planogram','Str_Merch_Lvl1_Planogram','Str_Merch_Lvl1_Planogram.csv')
Bq_to_GCS_3_Target_Str_Merch_Lvl2_Planogram = create_bq_to_gcs_task('3_Export_Str_Merch_Lvl2_Planogram','Str_Merch_Lvl2_Planogram','Str_Merch_Lvl2_Planogram','Str_Merch_Lvl2_Planogram.csv')
Bq_to_GCS_3_Target_Str_Merch_Lvl3_Planogram = create_bq_to_gcs_task('3_Export_Str_Merch_Lvl3_Planogram','Str_Merch_Lvl3_Planogram','Str_Merch_Lvl3_Planogram','Str_Merch_Lvl3_Planogram.csv')

# Task Flow Dependencies 
Sensor_Store << Bq_3_1_Stage_Planogram
Sensor_Store >> Sensor_Merch_Level1
Sensor_Store >> Sensor_Merch_Level2
#Sensor_Store >> Sensor_Merch_Level3

Sensor_Merch_Level1 >> Bq_3_2_Target_Str_Merch_Lvl1_Planogram >> Bq_to_GCS_3_Target_Str_Merch_Lvl1_Planogram
Sensor_Merch_Level2 >> Bq_3_3_Target_Str_Merch_Lvl2_Planogram >> Bq_to_GCS_3_Target_Str_Merch_Lvl2_Planogram
Sensor_Merch_Level3 >> Bq_3_4_Target_Str_Merch_Lvl3_Planogram >> Bq_to_GCS_3_Target_Str_Merch_Lvl3_Planogram


## Handshake Check ##
handshake_task = DummyOperator(
    task_id='success',
    dag=dag
)

[Bq_to_GCS_1_GCS_Target_Str_Merch_Lvl3_Adjacency, Bq_to_GCS_2_GCS_Target_Merch_Level_Capacity, Bq_to_GCS_3_Target_Str_Merch_Lvl1_Planogram, Bq_to_GCS_3_Target_Str_Merch_Lvl2_Planogram, Bq_to_GCS_3_Target_Str_Merch_Lvl3_Planogram] >> handshake_task