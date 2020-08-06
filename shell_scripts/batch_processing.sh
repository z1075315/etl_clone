#!/bin/bash

#Load configuration file
. ../config/etl_config.cfg

file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"batch_processing_log_"$file_date".log"

echo " `date -u` : Starting Batch Processing" >> $logfile

echo " `date -u` : Starting - fn_beta_plan_analysis_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_analysis_archived()"
echo " `date -u` : Ending   - fn_beta_plan_analysis_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_approved_historic" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_approved_historic()"
echo " `date -u` : Ending   - fn_beta_plan_approved_historic" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_archive_delete" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_archive_delete()"
echo " `date -u` : Ending   - fn_beta_plan_archive_delete" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_completed_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_completed_archived()"
echo " `date -u` : Ending   - fn_beta_plan_completed_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_draft_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_draft_archived()"
echo " `date -u` : Ending   - fn_beta_plan_draft_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_editbeta_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_editbeta_archived()"
echo " `date -u` : Ending   - fn_beta_plan_editbeta_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_failed_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_failed_archived()"
echo " `date -u` : Ending   - fn_beta_plan_failed_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_hard_delete" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_hard_delete()"
echo " `date -u` : Ending   - fn_beta_plan_hard_delete" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_historic_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_historic_archived()"
echo " `date -u` : Ending   - fn_beta_plan_historic_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_pngappr_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_pngappr_archived()"
echo " `date -u` : Ending   - fn_beta_plan_pngappr_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_rejected_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_rejected_archived()"
echo " `date -u` : Ending   - fn_beta_plan_rejected_archived" >> $logfile 

echo " `date -u` : Starting - fn_beta_plan_wip_to_failed" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_beta_plan_wip_to_failed()"
echo " `date -u` : Ending   - fn_beta_plan_wip_to_failed" >> $logfile 

echo " `date -u` : Starting - fn_delete_archived_beta_plan" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_delete_archived_beta_plan()"
echo " `date -u` : Ending   - fn_delete_archived_beta_plan" >> $logfile 

echo " `date -u` : Starting - fn_delete_archived_beta_results" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_delete_archived_beta_results()"
echo " `date -u` : Ending   - fn_delete_archived_beta_results" >> $logfile 

echo " `date -u` : Starting - fn_delete_archived_opt_plans" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_delete_archived_opt_plans()"
echo " `date -u` : Ending   - fn_delete_archived_opt_plans" >> $logfile 

echo " `date -u` : Starting - fn_delete_archived_opt_results" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_delete_archived_opt_results()"
echo " `date -u` : Ending   - fn_delete_archived_opt_results" >> $logfile 
___='
echo " `date -u` : Starting - fn_ins_batch_audit_log" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_ins_batch_audit_log()"
echo " `date -u` : Ending   - fn_ins_batch_audit_log" >> $logfile 
'
echo " `date -u` : Starting - fn_opt_plan_analysis_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_analysis_archived()"
echo " `date -u` : Ending   - fn_opt_plan_analysis_archived" >> $logfile 

echo " `date -u` : Starting - fn_opt_plan_approved_live" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_approved_live()"
echo " `date -u` : Ending   - fn_opt_plan_approved_live" >> $logfile 

echo " `date -u` : Starting - fn_opt_plan_archive_delete" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_archive_delete()"
echo " `date -u` : Ending   - fn_opt_plan_archive_delete" >> $logfile 

echo " `date -u` : Starting - fn_opt_plan_completed_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_completed_archived()"
echo " `date -u` : Ending   - fn_opt_plan_completed_archived" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_draft_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_draft_archived()"
echo " `date -u` : Ending   - fn_opt_plan_draft_archived" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_failed_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_failed_archived()"
echo " `date -u` : Ending   - fn_opt_plan_failed_archived" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_hard_delete" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_hard_delete()"
echo " `date -u` : Ending   - fn_opt_plan_hard_delete" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_historic_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_historic_archived()"
echo " `date -u` : Ending   - fn_opt_plan_historic_archived" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_live_historic" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_live_historic()"
echo " `date -u` : Ending   - fn_opt_plan_live_historic" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_pngappr_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_pngappr_archived()"
echo " `date -u` : Ending   - fn_opt_plan_pngappr_archived" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_rejected_archived" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_rejected_archived()"
echo " `date -u` : Ending   - fn_opt_plan_rejected_archived" >> $logfile

echo " `date -u` : Starting - fn_opt_plan_wip_to_failed" >> $logfile 
psql -X -A -h $HOST $DBUSER -d $DBNAME --set ON_ERROR_STOP=on  -t -c "SELECT   batch.fn_opt_plan_wip_to_failed()"
echo " `date -u` : Ending   - fn_opt_plan_wip_to_failed" >> $logfile

echo " `date -u` : Ending Batch Processing" >> $logfile
