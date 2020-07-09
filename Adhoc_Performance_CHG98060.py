"""
### Adhoc_Performance_CHG98060 

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
schedule_interval = Variable.get("schedule_interval_full_load")
start_date = Variable.get("start_date",deserialize_json=True)


## Default Arguments ##
default_args = { 
    'owner': 'Adhoc_Performance_CHG98060',
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
    'Adhoc_Performance_CHG98060', default_args=default_args,catchup = False, schedule_interval=None)
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
    '10_Target_Perf_Weekly_Merch_Level1',
    '11_Target_Perf_Weekly_Merch_Level2',
    '12_Target_Perf_Weekly_Merch_Level3']

def create_bq_task(task_name):
    return BigQueryOperator(
    task_id=task_name,
    write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/sql/Adhoc_Performance_CHG98060/{}.sql'.format(task_name),
    params=dag_config,
    dag=dag)
    
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
Bq_1_10_Target_Perf_Weekly_Merch_Level1 = create_bq_task('1_10_Target_Perf_Weekly_Merch_Level1')
Bq_1_11_Target_Perf_Weekly_Merch_Level2 = create_bq_task('1_11_Target_Perf_Weekly_Merch_Level2')
Bq_1_12_Target_Perf_Weekly_Merch_Level3 = create_bq_task('1_12_Target_Perf_Weekly_Merch_Level3')

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

Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final >> Bq_1_10_Target_Perf_Weekly_Merch_Level1
Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final >> Bq_1_11_Target_Perf_Weekly_Merch_Level2
Bq_1_9_Stage_Weekwise_Merch_Hierarchy_Performance_Final >> Bq_1_12_Target_Perf_Weekly_Merch_Level3


success_task = DummyOperator(
    task_id='success',
    dag=dag
)

[Bq_1_10_Target_Perf_Weekly_Merch_Level1,Bq_1_11_Target_Perf_Weekly_Merch_Level2,Bq_1_12_Target_Perf_Weekly_Merch_Level3] >> success_task