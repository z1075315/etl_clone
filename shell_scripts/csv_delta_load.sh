#!/bin/bash
#CSV DELTA LOAD PROCESSS
csv_data_load_fun()
{

 var=`gsutil ls -r $file_path`
 if [ $? -ne 0 ]; then
    echo " `date -u` : " $file_path "Error in loacting CSV file in Cloud Storage Bucket. " >> $logfile
    exit 1
 fi
 echo $var
 echo "Target table - "$tgt_table
 echo "file path - "$file_path
 for file_name in $var
 do
   output=$(yes | gcloud sql import csv $database_ins $file_name --database=$DBNAME --table=$tgt_table)
  
   if [ $? -ne 0 ]; then
		echo " `date -u` : " $file_name "Error in loading CSV File into Cloud SQL." >> $logfile
		exit 1
   fi
 done
 echo "Load completed"
}

 #Load configuration file
 . ../config/etl_config.cfg


file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"csv_delta_load_log_"$file_date".log"
echo " `date -u` : Starting DELTA CSV data load process">> $logfile
echo " `date -u` : Starting - Delete CSV table data" >> $logfile
var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_preprocessor_full_data() as ret" )
if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delete CSV tables data. Please reason under staging.error_logs table." >> $logfile   
    
	exit 1
fi
echo " `date -u` : Ending - Delete CSV table data" >> $logfile

echo " `date -u` : Starting CSV data load - Delta">> $logfile

tgt_table='staging.csv_store'
file_path=$etl_delta_path'store/*.csv'

echo " `date -u` : Starting CSV delta data load for Store">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta data load for Store">> $logfile

tgt_table='staging.csv_store_demographics'
file_path=$etl_delta_path'store_demographics/*.csv'

echo " `date -u` : Starting CSV data load for Store Demographics ">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for Store Demographics ">> $logfile


tgt_table='staging.csv_merch_level1'
file_path=$etl_delta_path'Merch_Level1/*.csv'

echo " `date -u` : Starting CSV delta data load for Merchandise Level 1">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta data load for Merchandise Level 1">> $logfile

tgt_table='staging.csv_merch_level2'
file_path=$etl_delta_path'Merch_Level2/*.csv'

echo " `date -u` : Starting CSV delta data load for Merchandise Level 2">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta data load for Merchandise Level 2">> $logfile

tgt_table='staging.csv_merch_level3'
file_path=$etl_delta_path'Merch_Level3/*.csv'

echo " `date -u` : Starting CSV delta data load for Merchandise Level 3">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta  data load for Merchandise Level 3">> $logfile
    
tgt_table='staging.csv_perf_weekly_merch_level1'
file_path=$etl_delta_path'Perf_Weekly_Merch_Level1/*.csv'

echo " `date -u` : Starting CSV delta data load for Performance Weekly Merchandise Level 1">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta data load for Performance Weekly Merchandise Level 1">> $logfile

tgt_table='staging.csv_perf_weekly_merch_level2'
file_path=$etl_delta_path'Perf_Weekly_Merch_Level2/*.csv'

echo " `date -u` : Starting CSV delta data load for Performance Weekly Merchandise Level 2">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta data load for Performance Weekly Merchandise Level 2">> $logfile

tgt_table='staging.csv_perf_weekly_merch_level3'
file_path=$etl_delta_path'Perf_Weekly_Merch_Level3/*.csv'

echo " `date -u` : Starting CSV delta data load for Performance Weekly Merchandise Level 3">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV delta data load for Performance Weekly Merchandise Level 3">> $logfile

tgt_table='staging.csv_str_merch_lvl1_planogram'
file_path=$etl_delta_path'Str_Merch_Lvl1_Planogram/*.csv'

echo " `date -u` : Starting CSV data load for Planogram Merchandise Level 1">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for Planogram Merchandise Level 1">> $logfile

tgt_table='staging.csv_str_merch_lvl2_planogram'
file_path=$etl_delta_path'Str_Merch_Lvl2_Planogram/*.csv'

echo " `date -u` : Starting CSV data load for Planogram Merchandise Level 2">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for Planogram Merchandise Level 2">> $logfile

tgt_table='staging.csv_str_merch_lvl3_planogram'
file_path=$etl_delta_path'Str_Merch_Lvl3_Planogram/*.csv'

echo " `date -u` : Starting CSV data load for Planogram Merchandise Level 3">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for Planogram Merchandise Level 3">> $logfile

tgt_table='staging.csv_str_merch_lvl3_adjacency'
file_path=$etl_delta_path'Str_Merch_Lvl3_Adjacency/*.csv'

echo " `date -u` : Starting CSV data load for Adjacency Merchandise Level 3">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for Adjacency Merchandise Level 3">> $logfile

tgt_table='staging.csv_str_merch_lvl3_skuefficiency'
file_path=$etl_delta_path'Str_Merch_Lvl3_SKU_Efficiency/*.csv'

echo " `date -u` : Starting CSV data load for SKU Efficiency Merchandise Level 3">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for SKU Efficiency Merchandise Level 3">> $logfile

tgt_table='staging.csv_merch_level_capacity'
file_path=$etl_delta_path'Merch_Level_Capacity/*.csv'

echo " `date -u` : Starting CSV data load for Merchandise Level Capacity">> $logfile
csv_data_load_fun
echo " `date -u` : Ending CSV data load for Merchandise Level Capacity">> $logfile

echo " `date -u` : Ending CSV Delta data load">> $logfile
