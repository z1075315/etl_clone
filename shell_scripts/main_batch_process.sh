#!/bin/bash 
#---------------------------------------------------------------
#MAIN BATCH PROCESSING SCRIPT - TRIGGERES ALL OTHER BATCH PROCESS SCRIPTS AFTER ETL MASTER TABLE LOAD
#---------------------------------------------------------------


#Load configuration file
. ../config/etl_config.cfg

echo "Main Batch Processing Script"

 today="$(date '+%Y-%m-%d')"
 echo $today
 logfile=$logfile_path"main_batch_processing_log_"$today".log"
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Main Batch Processing Begins...." >> $logfile 
 echo "-------------------------------------------------------" >> $logfile
__=' 
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Touchfile Validation Script " >> $logfile
 sh $script_path/Touchfile_Validation_batch_processing.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Touchfile Validation Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Touchfile Validation Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi
'
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Post Processing Data Load Script " >> $logfile
 sh $script_path/post_processing_data.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Post Processing Data Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Post Processing Data Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi


 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Batch Processing Script " >> $logfile
 sh $script_path/batch_processing.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Batch Processing Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Batch Processing Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi


 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Materialized View Load Script " >> $logfile
 sh $script_path/materialized_view.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Stage Materialized View Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Stage Materialized View Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi
 

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Main Batch Processing Script  Ends...." >> $logfile 
 echo "-------------------------------------------------------" >> $logfile

