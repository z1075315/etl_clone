#!/bin/bash

#Load configuration file
. ../config/etl_config.cfg

file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"prototyping_store_jobs_log_"$file_date".log"

echo " `date -u` : Starting Prototyping Store Jobs" >> $logfile

echo " `date -u` : Starting - fn_buddy_str_ml1" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_buddy_str_ml1()"
echo " `date -u` : Ending   - fn_buddy_str_ml1" >> $logfile 

echo " `date -u` : Starting - fn_buddy_str_ml2" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_buddy_str_ml2()"
echo " `date -u` : Ending   - fn_buddy_str_ml2" >> $logfile 

echo " `date -u` : Starting - fn_buddy_str_ml3" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_buddy_str_ml3()"
echo " `date -u` : Ending   - fn_buddy_str_ml3" >> $logfile 

echo " `date -u` : Starting - fn_adm_merch_level1_data_parameter" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_adm_merch_level1_data_parameter()"
echo " `date -u` : Ending   - fn_adm_merch_level1_data_parameter" >> $logfile 

echo " `date -u` : Starting - fn_adm_merch_level2_data_parameter" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_adm_merch_level2_data_parameter()"
echo " `date -u` : Ending   - fn_adm_merch_level2_data_parameter" >> $logfile 

echo " `date -u` : Starting - fn_adm_merch_level3_data_parameter" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   admin.fn_adm_merch_level3_data_parameter()"
echo " `date -u` : Ending   - fn_adm_merch_level3_data_parameter" >> $logfile 

echo " `date -u` : Starting - fn_populate_vp_merch_smry" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   master.fn_populate_vp_merch_smry()"
echo " `date -u` : Ending   - fn_populate_vp_merch_smry" >> $logfile 

echo " `date -u` : Ending Prototyping Store Jobs" >> $logfile
