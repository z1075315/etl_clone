"""
### PERFORMANCE_WEEKLY_SPACELEVEL and SKU_EFFICIENCY

ETL
"""

from airflow import DAG
from datetime import datetime, timedelta,date
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

## Global Variables ##
dag_config = Variable.get("ENV_Variables", deserialize_json=True)
email = Variable.get("email",deserialize_json=True)
schedule_interval = Variable.get("schedule_interval_delta")
start_date = Variable.get("start_date",deserialize_json=True)

## Default Arguments ##
default_args = { 
    'owner': 'airflow',
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
    'top_delta_perf_spc_wk_sku_eff', default_args=default_args,catchup = False, schedule_interval=schedule_interval)
dag.doc_md = __doc__

Performance_Weekly_Spacelevel_bigquery_task_ids = ['1_Stage_Weekwise_Merch_Hierarchy',
    '2_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE',
    '3_Stage_Weekwise_Merch_Hierarchy_Article_UOMs',
    '4_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Store_Capacity',
    '5_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share',
    '6_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share',          
    '7_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg',
    '8_Stage_Weekwise_Merch_Hierarchy_Space_Performance',
    '9_Stage_Weekwise_Merch_Hierarchy_Performance_Final',
    '10_Archive_Target',
    '11_Target_Perf_Weekly_Merch_Level1',
    '12_Target_Perf_Weekly_Merch_Level2',
    '13_Target_Perf_Weekly_Merch_Level3',
    '14_Delta_Perf_Weekly_Merch_Level1',
    '15_Delta_Perf_Weekly_Merch_Level2',
    '16_Delta_Perf_Weekly_Merch_Level3']

Sku_Effeciency_bigquery_task_ids = ['1_Stage_SKU_Efficiency_Space',
    '2_Stage_SKU_Efficiency_Article_Sales',
    '3_Stage_SKU_Efficiency',
    '4_Target_Str_Merch_Lvl3_SKU_Efficiency']

def create_bq_task(task_name):
    return BigQueryOperator(
    task_id=task_name,
    write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/top_delta_perf_spc_wk_sku_eff/{}.sql'.format(task_name),
    params=dag_config,
    dag=dag)

def create_sensor_task(task_name,external_dag_id,external_task_id):
    return ExternalTaskSensor(task_id='sensor'+task_name, 
    external_dag_id = external_dag_id, 
    external_task_id = external_task_id, 
    dag=dag)

def create_bq_to_gcs_task(task_name,source_table_name,folder,file_name):
    return BigQueryToCloudStorageOperator(task_id = task_name,
    source_project_dataset_table = "{{ var.json.ENV_Variables.Optumera_Target_DS }}."+source_table_name,
    destination_cloud_storage_uris = "{{ var.json.ENV_Variables.Export_URI_DeltaLoad }}/"+folder+"/"+file_name,
    export_format = 'CSV',
    field_delimiter = ',',
    replace = True,
    print_header = False,
    dag = dag)
### External Task Sensors 
Sensor_Store_Perf = create_sensor_task('Store_Perf','top_delta_mstr_str_merch','1_6_Target_Store')
Sensor_Merch_Level1 = create_sensor_task('Merch_Level1','top_delta_mstr_str_merch','2_7_Target_Merch_Level1')
Sensor_Merch_Level2 = create_sensor_task('Merch_Level2','top_delta_mstr_str_merch','2_8_Target_Merch_Level2')
Sensor_Merch_Level3 = create_sensor_task('Merch_Level3','top_delta_mstr_str_merch','2_9_Target_Merch_Level3')
Sensor_Store_Sku_Eff = create_sensor_task('Store_Sku_Eff','top_delta_mstr_str_merch','1_6_Target_Store')
Sensor_Merch_Level3_Sku_Eff = create_sensor_task('Merch_Level3_Sku_Eff','top_delta_mstr_str_merch','2_9_Target_Merch_Level3')
    
###PERFORMANCE_WEEKLY_SPACELEVEL tasks
Bq_1_1_Stage_Weekwise_Merch_Hierarchy = create_bq_task('1_1_Stage_Weekwise_Merch_Hierarchy')
Bq_1_2_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE = create_bq_task('1_2_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE')
Bq_1_3_Stage_Weekwise_Merch_Hierarchy_Article_UOMs = create_bq_task('1_3_Stage_Weekwise_Merch_Hierarchy_Article_UOMs')
Bq_1_4_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Store_Capacity = create_bq_task('1_4_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Store_Capacity')
Bq_1_5_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share = create_bq_task('1_5_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share')
Bq_1_6_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share = create_bq_task('1_6_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share')
Bq_1_7_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg = create_bq_task('1_7_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg')
Bq_1_8_Stage_Weekwise_Merch_Hierarchy_Space_Performance = create_bq_task('1_8_Stage_Weekwise_Merch_Hierarchy_Space_Performance')
Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final = create_bq_task('1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final')
Bq_1_10_Archive_Target = create_bq_task('1_10_Archive_Target')
Bq_1_11_Target_Perf_Weekly_Merch_Level1 = create_bq_task('1_11_Target_Perf_Weekly_Merch_Level1')
Bq_1_12_Target_Perf_Weekly_Merch_Level2 = create_bq_task('1_12_Target_Perf_Weekly_Merch_Level2')
Bq_1_13_Target_Perf_Weekly_Merch_Level3 = create_bq_task('1_13_Target_Perf_Weekly_Merch_Level3')
Bq_1_14_Delta_Perf_Weekly_Merch_Level1 = create_bq_task('1_14_Delta_Perf_Weekly_Merch_Level1')
Bq_1_15_Delta_Perf_Weekly_Merch_Level2 = create_bq_task('1_15_Delta_Perf_Weekly_Merch_Level2')
Bq_1_16_Delta_Perf_Weekly_Merch_Level3 = create_bq_task('1_16_Delta_Perf_Weekly_Merch_Level3')

Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level1 = create_bq_to_gcs_task('1_GCS_Target_Perf_Weekly_Merch_Level1','Delta_Perf_Weekly_Merch_Level1','Perf_Weekly_Merch_Level1','Perf_Weekly_Merch_Level1*.csv')
Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level2 = create_bq_to_gcs_task('1_GCS_Target_Perf_Weekly_Merch_Level2','Delta_Perf_Weekly_Merch_Level2','Perf_Weekly_Merch_Level2','Perf_Weekly_Merch_Level2*.csv')
Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level3 = create_bq_to_gcs_task('1_GCS_Target_Perf_Weekly_Merch_Level3','Delta_Perf_Weekly_Merch_Level3','Perf_Weekly_Merch_Level3','Perf_Weekly_Merch_Level3*.csv')

###DOS ERROR tasks
Bq_3_1_DOS_Weekwise_Store_Article_Capacity = create_bq_task('3_1_DOS_Weekwise_Store_Article_Capacity')
Bq_3_2_DOS_Weekwise_Store_Article_Avg_Sales = create_bq_task('3_2_DOS_Weekwise_Store_Article_Avg_Sales')
Bq_3_3_Weekwise_Merch_Hierarchy_DOS_Error = create_bq_task('3_3_Weekwise_Merch_Hierarchy_DOS_Error')
Bq_3_4_Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT = create_bq_task('3_4_Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT')

###Assortment Coverage
Bq_4_1_Stage_Weekwise_Assort_Covrg = create_bq_task('4_1_Stage_Weekwise_Assort_Covrg')

###Set dependencies for PERFORMANCE_WEEKLY_SPACELEVEL ,Assortment Coverage and DOS Error tasks
Bq_1_1_Stage_Weekwise_Merch_Hierarchy >> [Bq_1_2_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE,Bq_1_3_Stage_Weekwise_Merch_Hierarchy_Article_UOMs,Bq_1_7_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg,Bq_1_8_Stage_Weekwise_Merch_Hierarchy_Space_Performance]

Bq_1_1_Stage_Weekwise_Merch_Hierarchy >> [Bq_3_1_DOS_Weekwise_Store_Article_Capacity,Bq_3_2_DOS_Weekwise_Store_Article_Avg_Sales]
Bq_3_3_Weekwise_Merch_Hierarchy_DOS_Error << [Bq_3_1_DOS_Weekwise_Store_Article_Capacity,Bq_3_2_DOS_Weekwise_Store_Article_Avg_Sales] 
Bq_3_3_Weekwise_Merch_Hierarchy_DOS_Error >> Bq_3_4_Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT

Bq_1_1_Stage_Weekwise_Merch_Hierarchy >> Bq_4_1_Stage_Weekwise_Assort_Covrg

Bq_1_3_Stage_Weekwise_Merch_Hierarchy_Article_UOMs >> Bq_1_4_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Store_Capacity >> Bq_1_5_Stage_Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share >> Bq_1_6_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share >> Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final


Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final << [Bq_1_2_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE,Bq_1_7_Stage_Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg,Bq_1_8_Stage_Weekwise_Merch_Hierarchy_Space_Performance,Bq_3_4_Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT,Bq_4_1_Stage_Weekwise_Assort_Covrg]


Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final >> Bq_1_10_Archive_Target
Sensor_Store_Perf << Bq_1_10_Archive_Target
Sensor_Store_Perf >> [Sensor_Merch_Level1,Sensor_Merch_Level2,Sensor_Merch_Level3]

Sensor_Merch_Level1 >> Bq_1_11_Target_Perf_Weekly_Merch_Level1 >> Bq_1_14_Delta_Perf_Weekly_Merch_Level1 >> Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level1
Sensor_Merch_Level2 >> Bq_1_12_Target_Perf_Weekly_Merch_Level2 >> Bq_1_15_Delta_Perf_Weekly_Merch_Level2 >> Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level2
Sensor_Merch_Level3 >> Bq_1_13_Target_Perf_Weekly_Merch_Level3 >> Bq_1_16_Delta_Perf_Weekly_Merch_Level3 >> Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level3

###SKU_EFFICIENCY tasks
Bq_2_1_Stage_SKU_Efficiency_Space = create_bq_task('2_1_Stage_SKU_Efficiency_Space')
Bq_2_2_Stage_SKU_Efficiency_Article_Sales = create_bq_task('2_2_Stage_SKU_Efficiency_Article_Sales')
Bq_2_3_Stage_SKU_Efficiency = create_bq_task('2_3_Stage_SKU_Efficiency')
Bq_2_4_Target_Str_Merch_Lvl3_SKU_Efficiency = create_bq_task('2_4_Target_Str_Merch_Lvl3_SKU_Efficiency')

Bq_to_GCS_2_Target_Str_Merch_Lvl3_SKU_Efficiency = create_bq_to_gcs_task('2_GCS_Target_Str_Merch_Lvl3_SKU_Efficiency','Str_Merch_Lvl3_SKU_Efficiency','Str_Merch_Lvl3_SKU_Efficiency','Str_Merch_Lvl3_SKU_Efficiency.csv')
###Set dependencies for SKU_EFFICIENCY
Bq_2_1_Stage_SKU_Efficiency_Space >> Bq_2_2_Stage_SKU_Efficiency_Article_Sales >> Bq_2_3_Stage_SKU_Efficiency
Bq_2_3_Stage_SKU_Efficiency >> Sensor_Store_Sku_Eff  >> Sensor_Merch_Level3_Sku_Eff
Sensor_Merch_Level3_Sku_Eff >> Bq_2_4_Target_Str_Merch_Lvl3_SKU_Efficiency >> Bq_to_GCS_2_Target_Str_Merch_Lvl3_SKU_Efficiency

success_task = DummyOperator(
    task_id='success',
    dag=dag
)

[Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level1,Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level2,Bq_to_GCS_1_Target_Perf_Weekly_Merch_Level3,Bq_to_GCS_2_Target_Str_Merch_Lvl3_SKU_Efficiency] >> success_task