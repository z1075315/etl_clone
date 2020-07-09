/*
1.The Latest 5 Weeks of Performance Data which is to be deleted is archived in the respective Arc_Perf_Weekly_Merch_Level tables 
  with _PARTITIONTIME as CURRENT_DATE
*/
INSERT
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Arc_Perf_Weekly_Merch_Level1` ( 
	WEEK_ID ,
	MERCH_LEVEL1_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	_PARTITIONTIME
)
(
SELECT
	WEEK_ID ,
	MERCH_LEVEL1_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	TIMESTAMP(CURRENT_DATE)  
FROM 
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level1`
WHERE 
	week_id in 
	(SELECT Week_id FROM `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` 
	WHERE Week_End_Date <= CURRENT_DATE order by week_id desc limit {{params.Weeks_to_Delete}}));

INSERT
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Arc_Perf_Weekly_Merch_Level2` ( 
	WEEK_ID ,
	MERCH_LEVEL2_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	_PARTITIONTIME
)
(
SELECT
	WEEK_ID ,
	MERCH_LEVEL2_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	TIMESTAMP(CURRENT_DATE)
FROM 
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level2` 
WHERE 
	week_id in (SELECT Week_id FROM `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` 
	WHERE Week_End_Date <= CURRENT_DATE order by week_id desc limit {{params.Weeks_to_Delete}}));

INSERT
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Arc_Perf_Weekly_Merch_Level3` ( 
	WEEK_ID ,
	MERCH_LEVEL3_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	DOS ,
	ASSRT_CVRG,
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	_PARTITIONTIME
)
(
SELECT
	WEEK_ID ,
	MERCH_LEVEL3_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	DOS ,
	ASSRT_CVRG,
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	TIMESTAMP(CURRENT_DATE)
FROM 
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level3` 
WHERE 
	week_id in (SELECT Week_id FROM `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` 
	WHERE Week_End_Date <= CURRENT_DATE order by week_id desc limit {{params.Weeks_to_Delete}}));