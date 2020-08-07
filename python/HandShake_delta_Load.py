from airflow import DAG
from datetime import datetime, timedelta,date
from airflow.operators import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable


## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_delta")
start_date = Variable.get("start_date",deserialize_json=True)

## Default Arguments ##
default_args = { 
    'owner': 'HandShake_delta_Load',
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
    'HandShake_delta_Load', catchup = False,default_args=default_args, schedule_interval=schedule_interval)


# Check Dags are completed
Check_Str_Merch = ExternalTaskSensor(
    task_id='Check_Str_Merch',
    external_dag_id = 'top_delta_mstr_str_merch',
    external_task_id = 'success',
    #allowed_states=None,
    execution_delta=None,
    dag = dag)
Check_Perf = ExternalTaskSensor(
    task_id='Check_Perf',
    external_dag_id = 'top_delta_perf_spc_wk_sku_eff',
    external_task_id = 'success',
    #allowed_states=None,
    execution_delta=None,
    dag = dag)
Check_Plano = ExternalTaskSensor(
    task_id='Check_Plano',
    external_dag_id = 'top_delta_adj_merch_lvl_cap_plng',
    external_task_id = 'success',
    #allowed_states=None,
    execution_delta=None,
    dag = dag)

'''Check_Demo = ExternalTaskSensor(
    task_id='Check_Plano',
    external_dag_id = 'top_delta_str_demog',
    external_task_id = 'Full_Load_GCS_Store_Demographics',
    #allowed_states=None,
    execution_delta=timedelta(minutes= -30),
    dag = dag)'''

GCS_Check = BashOperator(
    task_id='GCS_Check',
    bash_command='touch Delta_Completed.txt;gsutil mv Delta_Completed.txt '+dag_config['Export_URI_DeltaLoad']+'/Delta_Completed.txt',
    dag=dag,
)
#[Check_Str_Merch,Check_Perf,Check_Plano,Check_Demo]>> GCS_Check
[Check_Str_Merch,Check_Perf,Check_Plano]>> GCS_Check