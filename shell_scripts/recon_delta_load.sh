#!/bin/bash 
#Reconciliation script to validate counts between GS bucket extarct and CSV table.

#Function to check counts between GS Bucket file and CSV Table.
csv_count_check_fun()
{
 gs_count=`gsutil cat $file_path | wc -l`
  if [ $? -ne 0 ]; then
    echo " `date -u` : GS Bucket count fetch failed " >> $logfile
    exit 1
 fi  

 sql_count=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select count(*) from $tgt_table")
 if [ $? -ne 0 ]; then
    echo " `date -u` : CSV Table Count Query failed " >> $logfile
    exit 1
 fi  

 echo " `date -u` : CSV File count - "$gs_count >> $logfile
 echo " `date -u` : CSV Table count - "$sql_count >> $logfile
 if [ $gs_count -eq $sql_count ]; then
    echo " `date -u` : Count Matched	 "  >> $logfile
 else
    echo " `date -u` : Count Not Matching " >> $logfile
    chk_flag='FALSE'
 fi
 
}

#Load configuration file
. ../config/etl_config.cfg

 echo "Reconciliation Script - delta load " 
 today="$(date '+%Y-%m-%d')"
 chk_flag='TRUE'
 echo $today
 logfile=$logfile_path"reconciliation_delta_log_"$today".log"
 echo " `date -u` : Delta Load Reconciliation  Validation Begins...." >> $logfile 

 tgt_table='staging.csv_store'
 file_path=$etl_delta_path'store/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Store">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Store">> $logfile

 tgt_table='staging.csv_store_demographics'
 file_path=$etl_delta_path'store_demographics/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Store Demographics">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Store Demographics">> $logfile

 tgt_table='staging.csv_merch_level1'
 file_path=$etl_delta_path'Merch_Level1/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Merchandise Level 1">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Merchandise Level 1">> $logfile

 tgt_table='staging.csv_merch_level2'
 file_path=$etl_delta_path'Merch_Level2/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Merchandise Level 2">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Merchandise Level 2">> $logfile

 tgt_table='staging.csv_merch_level3'
 file_path=$etl_delta_path'Merch_Level3/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Merchandise Level 3">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Merchandise Level 3">> $logfile

 tgt_table='staging.csv_perf_weekly_merch_level1'
 file_path=$etl_delta_path'Perf_Weekly_Merch_Level1/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Performance Weekly Merchandise Level 1">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Performance Weekly Merchandise Level 1">> $logfile

 tgt_table='staging.csv_perf_weekly_merch_level2'
 file_path=$etl_delta_path'Perf_Weekly_Merch_Level2/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Performance Weekly Merchandise Level 2">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Performance Weekly Merchandise Level 2">> $logfile

 tgt_table='staging.csv_perf_weekly_merch_level3'
 file_path=$etl_delta_path'Perf_Weekly_Merch_Level3/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Performance Weekly Merchandise Level 3">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Performance Weekly Merchandise Level 3">> $logfile

 tgt_table='staging.csv_str_merch_lvl1_planogram'
 file_path=$etl_delta_path'Str_Merch_Lvl1_Planogram/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Planogram Merchandise Level 1">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Planogram Merchandise Level 1">> $logfile

 tgt_table='staging.csv_str_merch_lvl2_planogram'
 file_path=$etl_delta_path'Str_Merch_Lvl2_Planogram/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Planogram Merchandise Level 2">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Planogram Merchandise Level 2">> $logfile

 tgt_table='staging.csv_str_merch_lvl3_planogram'
 file_path=$etl_delta_path'Str_Merch_Lvl3_Planogram/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Planogram Merchandise Level 3">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Planogram Merchandise Level 3">> $logfile

 tgt_table='staging.csv_str_merch_lvl3_adjacency'
 file_path=$etl_delta_path'Str_Merch_Lvl3_Adjacency/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Adjacency Merchandise Level 3">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Adjacency Merchandise Level 3">> $logfile

 tgt_table='staging.csv_str_merch_lvl3_skuefficiency'
 file_path=$etl_delta_path'Str_Merch_Lvl3_SKU_Efficiency/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - SKU Efficiency Merchandise Level 3">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - SKU Efficiency Merchandise Level 3">> $logfile

 tgt_table='staging.csv_merch_level_capacity'
 file_path=$etl_delta_path'Merch_Level_Capacity/*.csv'

 echo " `date -u` : Starting GS bucket and CSV table count check - Merchandise Level Capacity">> $logfile
 csv_count_check_fun
 echo " `date -u` : Completed GS bucket and CSV table count check  - Merchandise Level Capacity">> $logfile

 
 
 
 
 echo " `date -u` : Check flag status : "$chk_flag  >> $logfile
 if [ $chk_flag = 'FALSE' ]; then
       echo " `date -u` : Delta Count check validation failed.... Investigate ETL Pipeline-I & CSV Table Load process " >> $logfile
       #set alert to notify support
       exit 1
 else
       echo " `date -u` : Delta Count check validation completed successfully.. Proceeding with Stage table Load"  >> $logfile
       exit 0
 fi



