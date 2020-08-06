#!/bin/bash
#DELTA PROCESS - STAGE DATA LOAD
#Load configuration file
. ../config/etl_config.cfg


file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"stage_delta_load_log_"$file_date".log"

echo " `date -u` : Starting - Delta Staging data load process...........">> $logfile
echo " `date -u` : Starting - Delta Staging Store Load" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_store() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Store data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Store Load" >> $logfile
echo " `date -u` : Starting - Staging Store Demographics Load" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_store_demographics() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging Store Demographics data load. Please reason under staging.error_logs table." >> $logfile   
	exit 1
fi

echo " `date -u` : Ending  - Staging Store Demographics Load" >> $logfile

echo " `date -u` : Starting - Delta Staging Merchandise Hierarchy Level 1" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_merch_level1() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Merchandise Hierarchy Level 1 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Merchandise Hierarchy Level 1" >> $logfile
echo " `date -u` : Starting - Delta Staging Merchandise Hierarchy Level 2" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_merch_level2() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Merchandise Hierarchy Level 2 data load. Please reason under staging.error_logs table." >> $logfile 
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Merchandise Hierarchy Level 2" >> $logfile
echo " `date -u` : Starting - Delta Staging Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_merch_level3() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Delta Staging Performance Weekly Merchandise Hierarchy Level 1" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_perf_weekly_merch_level1() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Performance Weekly Merchandise Hierarchy Level 1 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Performance Weekly Merchandise Hierarchy Level 1" >> $logfile
echo " `date -u` : Starting - Delta Staging Performance Weekly Merchandise Hierarchy Level 2" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_perf_weekly_merch_level2() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Performance Weekly Merchandise Hierarchy Level 2 data load. Please reason under staging.error_logs table." >> $logfile  
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Performance Weekly Merchandise Hierarchy Level 2" >> $logfile
echo " `date -u` : Starting - Delta Staging Performance Weekly Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_delta_load_to_stg_perf_weekly_merch_level3() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Delta Staging Performance Weekly Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Delta Staging Performance Weekly Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Staging Planogram Merchandise Hierarchy Level 1" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_str_merch_lvl1_planogram() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging Planogram Merchandise Hierarchy Level 1 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Staging Planogram Merchandise Hierarchy Level 1" >> $logfile
echo " `date -u` : Starting - Staging Planogram Merchandise Hierarchy Level 2" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_str_merch_lvl2_planogram() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging Planogram Merchandise Hierarchy Level 2 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Staging Planogram Merchandise Hierarchy Level 2" >> $logfile
echo " `date -u` : Starting - Staging Planogram Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_str_merch_lvl3_planogram() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging Planogram Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1	
fi

echo " `date -u` : Ending  - Staging Planogram Merchandise Hierarchy  Level 3" >> $logfile
echo " `date -u` : Starting - Staging Adjacency Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_str_merch_lvl3_adjacency() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging Adjacency Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Staging Adjacency Merchandise Hierarchy Level 3" >> $logfile

echo " `date -u` : Starting - Staging SKU Efficiency Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_str_merch_lvl3_skuefficiency() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging SKU Efficiency Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Staging SKU Efficiency Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Staging Merchandise Hierarchy Level Capacity" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_stg_merch_level_capacity() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted Staging Merchandise Hierarchy Level Capacity data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending  - Staging Merchandise Hierarchy Level Capacity" >> $logfile
echo " `date -u` : Ending  - Delta Staging Full Data load" >> $logfile
echo " -----------------------------------------------------------------------------------" >> $logfile
echo " -----------------------------------------------------------------------------------" >> $logfile
