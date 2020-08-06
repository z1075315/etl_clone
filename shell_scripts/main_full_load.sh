#!/bin/bash 
#---------------------------------------------------------------
#MAIN FULL LOAD SCRIPT - TRIGGERES ALL OTHER FULL LOAD SCRIPTS
#---------------------------------------------------------------


#Load configuration file
. ../config/etl_config.cfg

echo "Main Full Load Script"

 today="$(date '+%Y-%m-%d')"
 echo $today
 
 logfile=$logfile_path"main_full_load_log_"$today".log"
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Main Full Load Process Begins...." >> $logfile 
 echo "-------------------------------------------------------" >> $logfile

___=' 
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Touchfile Validation Script " >> $logfile
 sh $script_path/Touchfile_Validation_full_load.sh
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
 echo " `date -u` : Invoke CSV Load Script " >> $logfile
 sh $script_path/csv_full_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : CSV Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : CSV Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Reconciliation Script " >> $logfile
 sh $script_path/recon_full_load.sh
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
 sh $script_path/stage_full_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Stage Table Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Stage Table Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi
 
 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Invoke Master Table Load Script " >> $logfile
 sh $script_path/master_full_load.sh
 shell_ret=$?
 if [ $shell_ret -eq 0 ]; then
 	echo " `date -u` : Master Table Load Script Completed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
 else
 	echo " `date -u` : Master Table Load Script Failed " >> $logfile
 	echo "-------------------------------------------------------" >> $logfile
	exit 1
 fi

 echo " `date -u` : Create Touchfile for DB Batch Processing " >> $logfile
 touch $touchfile_path"/Master_Tbl_Load_Completed-"$today".txt"

 echo "-------------------------------------------------------" >> $logfile
 echo " `date -u` : Main Full Load Process Ends...." >> $logfile 
 echo "-------------------------------------------------------" >> $logfile

