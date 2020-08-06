#!/bin/bash

#Load configuration file
. ../config/etl_config.cfg

file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"post_processing_data_load_log_"$file_date".log"

echo " `date -u` : Starting After ETL Table Refresh" >> $logfile


echo " `date -u` : Starting - fn_perf_aggr_all_merch_lvls" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_perf_aggr_all_merch_lvls()"
echo " `date -u` : Ending   - fn_perf_aggr_all_merch_lvls" >> $logfile 

echo " `date -u` : Starting - fn_populate_store_merch_level1_footage" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_store_merch_level1_footage()"
echo " `date -u` : Ending   - fn_populate_store_merch_level1_footage" >> $logfile 

echo " `date -u` : Starting - fn_populate_store_merch_level2_footage" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_store_merch_level2_footage()"
echo " `date -u` : Ending   - fn_populate_store_merch_level2_footage" >> $logfile 

echo " `date -u` : Starting - fn_populate_store_merch_level3_footage" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_store_merch_level3_footage()"
echo " `date -u` : Ending   - fn_populate_store_merch_level3_footage" >> $logfile 

echo " `date -u` : Starting - fn_seasonality_scaling_ml1" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_seasonality_scaling_ml1()"
echo " `date -u` : Ending   - fn_seasonality_scaling_ml1" >> $logfile 

echo " `date -u` : Starting - fn_seasonality_scaling_ml2" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_seasonality_scaling_ml2()"
echo " `date -u` : Ending   - fn_seasonality_scaling_ml2" >> $logfile 

echo " `date -u` : Starting - fn_seasonality_scaling_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_seasonality_scaling_ml3()"
echo " `date -u` : Ending   - fn_seasonality_scaling_ml3" >> $logfile 

echo " `date -u` : Starting - fn_seasonality_index_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_seasonality_index_ml1()"
echo " `date -u` : Ending   - fn_seasonality_index_ml1" >> $logfile 

echo " `date -u` : Starting - fn_seasonality_index_ml2" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_seasonality_index_ml2()"
echo " `date -u` : Ending   - fn_seasonality_index_ml2" >> $logfile 

echo " `date -u` : Starting - fn_seasonality_index_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_seasonality_index_ml3()"
echo " `date -u` : Ending   - fn_seasonality_index_ml3" >> $logfile 

echo " `date -u` : Starting - fn_ml1_deseasonalisation" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_ml1_deseasonalisation()"
echo " `date -u` : Ending   - fn_ml1_deseasonalisation" >> $logfile 

echo " `date -u` : Starting - fn_ml2_deseasonalisation" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_ml2_deseasonalisation()"
echo " `date -u` : Ending   - fn_ml2_deseasonalisation" >> $logfile 

echo " `date -u` : Starting - fn_ml3_deseasonalisation" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_ml3_deseasonalisation()"
echo " `date -u` : Ending   - fn_ml3_deseasonalisation" >> $logfile 

echo " `date -u` : Starting - fn_forecast_deseasonalisation_ml1" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_forecast_deseasonalisation_ml1()"
echo " `date -u` : Ending   - fn_forecast_deseasonalisation_ml1" >> $logfile 

echo " `date -u` : Starting - fn_forecast_deseasonalisation_ml2" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_forecast_deseasonalisation_ml2()"
echo " `date -u` : Ending   - fn_forecast_deseasonalisation_ml2" >> $logfile 

echo " `date -u` : Starting - fn_forecast_deseasonalisation_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_forecast_deseasonalisation_ml3()"
echo " `date -u` : Ending   - fn_forecast_deseasonalisation_ml3" >> $logfile 

echo " `date -u` : Starting - fn_populate_capacity_modifier" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_capacity_modifier()"
echo " `date -u` : Ending   - fn_populate_capacity_modifier" >> $logfile 

echo " `date -u` : Starting - fn_populate_vp_merch_smry" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_vp_merch_smry()"
echo " `date -u` : Ending   - fn_populate_vp_merch_smry" >> $logfile 

echo " `date -u` : Starting - fn_populate_customer_modifier_ml1" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_customer_modifier_ml1()"
echo " `date -u` : Ending   - fn_populate_customer_modifier_ml1" >> $logfile 

echo " `date -u` : Starting - fn_populate_customer_modifier_ml2" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_customer_modifier_ml2()"
echo " `date -u` : Ending   - fn_populate_customer_modifier_ml2" >> $logfile 

echo " `date -u` : Starting - fn_populate_customer_modifier_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_customer_modifier_ml3()"
echo " `date -u` : Ending   - fn_populate_customer_modifier_ml3" >> $logfile 

echo " `date -u` : Starting - fn_buddy_str_ml1" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_buddy_str_ml1()"
echo " `date -u` : Ending   - fn_buddy_str_ml1" >> $logfile

echo " `date -u` : Starting - fn_buddy_str_ml2" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_buddy_str_ml2()"
echo " `date -u` : Ending   - fn_buddy_str_ml2" >> $logfile

echo " `date -u` : Starting - fn_buddy_str_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_buddy_str_ml3()"
echo " `date -u` : Ending   - fn_buddy_str_ml3" >> $logfile

echo " `date -u` : Starting - fn_populate_mrch_data_param" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT admin.fn_populate_mrch_data_param()"
echo " `date -u` : Ending   - fn_populate_mrch_data_param" >> $logfile

echo " `date -u` : Ending After ETL Table Refresh" >> $logfile
