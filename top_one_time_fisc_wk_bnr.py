from airflow import DAG
from datetime import datetime, timedelta,date
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.models import Variable

## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_full_load")
start_date = Variable.get("start_date",deserialize_json=True)

## Default Arguments ##
default_args = { 
    'owner': 'top_one_time_fisc_wk_bnr',
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
    'top_one_time_fisc_wk_bnr', catchup = False, default_args=default_args, schedule_interval='@once')

# BigQuery Source to BigQuery Stage
One_Time_Stage_Fiscal_Week =  BigQueryOperator(
    task_id='One_Time_Stage_Fiscal_Week',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_one_time_fisc_wk_bnr/1_1_Stage_Fiscal_Week.sql',
	params=dag_config,
	dag=dag)

# BigQuery Stage to BigQuery Target
One_Time_Target_Fiscal_Week =  BigQueryOperator(
    task_id='One_Time_Target_Fiscal_Week',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_one_time_fisc_wk_bnr/1_2_Target_Fiscal_Week.sql',
	params=dag_config,
	dag=dag)

# BigQuery Target to Google Cloud Storage
One_Time_GCS_Fiscal_Week = BigQueryToCloudStorageOperator(
    task_id = 'One_Time_GCS_Fiscal_Week',
    source_project_dataset_table = "{{var.json.ENV_Variables.Optumera_Target_DS}}"+".Fiscal_Week",
    destination_cloud_storage_uris = "{{var.json.ENV_Variables.Export_URI_FullLoad}}"+"/fiscal_week/fiscal_week.csv",
    export_format = 'CSV',
    field_delimiter = ',',
    replace = True,
    print_header = False,
    dag = dag
)






# BigQuery Source to BigQuery Stage
One_Time_Stage_Banner =  BigQueryOperator(
    task_id='One_Time_Stage_Banner',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_one_time_fisc_wk_bnr/2_1_Stage_Banner.sql',
	params=dag_config,
	dag=dag)

# BigQuery Stage to BigQuery Target
One_Time_Target_Banner =  BigQueryOperator(
    task_id='One_Time_Target_Banner',
	write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_one_time_fisc_wk_bnr/2_2_Target_Banner.sql',
    params=dag_config,
	dag=dag)

# BigQuery Target to Google Cloud Storage
One_Time_GCS_Banner = BigQueryToCloudStorageOperator(
    task_id = 'One_Time_GCS_Banner',
    source_project_dataset_table = "{{ var.json.ENV_Variables.Optumera_Target_DS }}"+".Banner",
    destination_cloud_storage_uris = "{{ var.json.ENV_Variables.Export_URI_FullLoad }}"+"/banner/banner.csv",
    export_format = 'CSV',
    field_delimiter = ',',
    replace = True,
    print_header = False,
    dag = dag
)

One_Time_Stage_Fiscal_Week >> One_Time_Target_Fiscal_Week >> One_Time_GCS_Fiscal_Week
One_Time_Stage_Banner >> One_Time_Target_Banner >> One_Time_GCS_Banner