#!/bin/bash 
#Touch file validation for DB Batch Processing ( Post ETL Master table load)

#Load configuration file
. ../config/etl_config.cfg

 echo "DB Batch Process Touchfile Validation Script"
 #echo $DBUSER $DBNAME $PGPASSWORD
 today="$(date '+%Y-%m-%d')"
 echo $today
 sleep_var=10
# today='2020-01-04'
 echo $today
 #logfile_path='/opt/apps/Macrospace/logs/'
 logfile=$logfile_path"validation_batch_process_log_"$today".log"
 echo " `date -u` : DB Batch Processing Touchfile Validation Begins...." >> $logfile 

 #Build today's file
 chk_filename=$touchfile_path'/Master_Tbl_Load_Completed-'$today'.txt'
 echo " `date -u` : Expected Touchfile - "$chk_filename  >> $logfile
 retry_cnt=1
 max_retry_cnt=2
 chk_flag='FALSE'
 #Check touchfile availability until defined retry and time delay
 while [ $retry_cnt -lt $max_retry_cnt ] 
 do 
	echo $retry_cnt
	         
	filename=`ls $chk_filename`
	#echo "file validation " $?
	if [ $? -eq 0 ]; then
	   chk_flag="TRUE"
      	   break
	else
	   chk_flag="FALSE"
	fi  
        echo $chk_flag	
	retry_cnt=`expr $retry_cnt + 1`
	sleep $sleep_var
 done
        echo " `date -u` : Check flag status : "$chk_flag  >> $logfile
 if [ $chk_flag = 'FALSE' ]; then
       echo " `date -u` : Touchfile not present to continue DB Batch Process " >> $logfile
       echo " `date -u` : Touchfile validation failed.... Investigate ETL Pipeline process " >> $logfile
       #set alert to notify support
       exit 1
 else
       echo " `date -u` : Touchfile exists.. Proceeding with DB Batch Process "  >> $logfile
       echo " `date -u` : Deleting... Touchfile, file name: "$chk_filename   >> $logfile
       rm $chk_filename 
       echo " `date -u` : DB Batch Process Touchfile validation completed"  >> $logfile
       exit 0
 fi


