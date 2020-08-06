#!/bin/bash 
#---------------------------------------------------------------
#MAIN DELTA LOAD SCRIPT - TRIGGERES ALL OTHER DELTA LOAD SCRIPTS
#---------------------------------------------------------------


#Load configuration file
. ../config/etl_config.cfg

echo "Main Delta Load Script"

 today="$(date '+%Y-%m-%d')"
 echo $today
 
 logfile=$logfile_path"main_delta_load_log_"$today".log"
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Main Delta Load Process Begins...." >> $logfile 
 echo "-------------------------------------------------------" >> $logfile
 
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Touchfile Validation Script " >> $logfile
 sh $script_path/Touchfile_Validation_delta_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Touchfile Validation Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Touchfile Validation Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi
 
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke CSV Delta Load Script " >> $logfile
 sh $script_path/csv_delta_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : CSV Delta Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : CSV Delta Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Reconciliation Script " >> $logfile
 sh $script_path/recon_delta_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : CSV file & CSV Table Reconciliation Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : CSV file & CSV Table Reconciliation Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Stage Table Load Script " >> $logfile
 sh $script_path/stage_delta_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Stage Table Delta Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Stage Table Delta Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Master Table Load Script " >> $logfile
 sh $script_path/master_delta_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Master Table Delta  Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Master Table Delta Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi
 
 echo " `date -u` : Create Touchfile for DB Batch Processing " >> $logfile
 touch $touchfile_path"/Master_Tbl_Load_Completed-"$today".txt"

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Main Delta Load Process Ends...." >> $logfile 
 echo "-------------------------------------------------------" >> $logfile

