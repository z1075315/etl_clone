#!/bin/bash
#MASTER FULL LOAD PROCESS
#Load configuration file
. ../config/etl_config.cfg


file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"master_full_load_log_"$file_date".log"

echo " `date -u` : Starting Master Full data load" >> $logfile
echo " `date -u` : Starting - Delete Master tables data" >> $logfile
var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_preprocessor_master_full_data() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Delete Master tables data. Please reason under staging.error_logs table." >> $logfile   
	exit 1
fi

echo " `date -u` : Ending - Delete Master tables data" >> $logfile
___='
echo " `date -u` : Starting - Master Banner Load" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_banner() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Banner data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Banner Load" >> $logfile

echo " `date -u` : Starting - Master Fiscal Week Load" >> $logfile


var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_fiscal_week() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Fiscal Week data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Fiscal Week Load" >> $logfile
'
echo " `date -u` : Starting - Master Store Load" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_store() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Store data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Store Load" >> $logfile
echo " `date -u` : Starting - Master Store Demographics Load" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_store_demographics() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Store Demographics data load. Please reason under staging.error_logs table." >> $logfile   
	exit 1
fi

echo " `date -u` : Ending - Master Store Demographics Load" >> $logfile
echo " `date -u` : Starting - Master Merchandise Hierarchy Level 1" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_merch_level1() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Merchandise Hierarchy Level 1 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Merchandise Hierarchy Level 1" >> $logfile
echo " `date -u` : Starting - Master Merchandise Hierarchy Level 2" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_merch_level2() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Merchandise Hierarchy Level 2 data load. Please reason under staging.error_logs table." >> $logfile 
	exit 1
fi

echo " `date -u` : Ending - Master Merchandise Hierarchy Level 2" >> $logfile
echo " `date -u` : Starting - Master Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_merch_level3() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Master Performance Weekly Merchandise Hierarchy Level 1" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_perf_weekly_merch_level1() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Performance Weekly Merchandise Hierarchy Level 1 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Performance Weekly Merchandise Hierarchy Level 1" >> $logfile
echo " `date -u` : Starting - Master Performance Weekly Merchandise Hierarchy Level 2" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_perf_weekly_merch_level2() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Performance Weekly Merchandise Hierarchy Level 2 data load. Please reason under staging.error_logs table." >> $logfile  
	exit 1
fi

echo " `date -u` : Ending - Master Performance Weekly Merchandise Hierarchy Level 2" >> $logfile
echo " `date -u` : Starting - Master Performance Weekly Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_perf_weekly_merch_level3() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Performance Weekly Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Performance Weekly Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Master Planogram Merchandise Hierarchy Level 1" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_str_merch_lvl1_planogram() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Planogram Merchandise Hierarchy Level 1 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Planogram Merchandise Hierarchy Level 1" >> $logfile
echo " `date -u` : Starting - Master Planogram Merchandise Hierarchy Level 2" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_str_merch_lvl2_planogram() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Planogram Merchandise Hierarchy Level 2 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Planogram Merchandise Hierarchy Level 2" >> $logfile
echo " `date -u` : Starting - Master Planogram Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_str_merch_lvl3_planogram() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Planogram Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1	
fi

echo " `date -u` : Ending - Master Planogram Merchandise Hierarchy  Level 3" >> $logfile
echo " `date -u` : Starting - Master Adjacency Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_str_merch_lvl3_adjacency() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Adjacency Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Adjacency Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Master SKU Efficiency Merchandise Hierarchy Level 3" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_str_merch_lvl3_skuefficiency() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master SKU Efficiency Merchandise Hierarchy Level 3 data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master SKU Efficiency Merchandise Hierarchy Level 3" >> $logfile
echo " `date -u` : Starting - Master Merchandise Hierarchy Level Capacity" >> $logfile

var=$(psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "select staging.fn_full_load_to_master_merch_level_capacity() as ret" )

if [ $var -ne 0 ]; then
    echo " `date -u` : Aborted - Master Merchandise Hierarchy Level Capacity data load. Please reason under staging.error_logs table." >> $logfile
	exit 1
fi

echo " `date -u` : Ending - Master Merchandise Hierarchy Level Capacity" >> $logfile
echo " `date -u` : Ending - Master Full Data load" >> $logfile
