#!/bin/bash

#Load configuration file
. ../config/etl_config.cfg

file_date="$(date '+%Y-%m-%d')"
logfile=$logfile_path"materialized_view_log_"$file_date".log"

echo " `date -u` : Starting Refresh MATERIALIZED VIEW" >> $logfile
psql -X -A -h $HOST $DBUSER -d $DBNAME << EOF
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl1_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl1_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl1_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl1_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl2_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl2_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl2_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl2_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl3_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl3_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl3_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl3_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl4_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl4_hist_beta_graph;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl4_historical_beta;
REFRESH MATERIALIZED VIEW beta_results.mvw_merch_lvl4_historical_beta;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml1;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml1;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml1_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml1_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml2;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml2;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml2_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml2_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml3;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml3;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml3_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml3_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml4;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml4;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml4_ps;
REFRESH MATERIALIZED VIEW opt_plan.mvw_imp_vs_recomm_rpt_ml4_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml1;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml1;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml1_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml1_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml2;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml2;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml2_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml2_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml3;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml3;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml3_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml3_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml4;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml4;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml4_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_inter_ml4_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml1;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml1;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml2;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml2;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml3;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml3;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml4;
REFRESH MATERIALIZED VIEW opt_result.mvw_plan_comprson_rpt_prty_str_inter_ml4;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml1;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml1;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml1_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml1_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml2;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml2;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml2_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml2_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml3;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml3;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml3_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml3_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml4;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml4;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml4_ps;
REFRESH MATERIALIZED VIEW opt_result.mvw_space_recomdtn_ml4_ps;
EOF
echo " `date -u` : Ending Refresh MATERIALIZED VIEW" >> $logfile
