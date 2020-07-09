/*
1.Truncate the Delta_Perf_Weekly_Merch_Level1 table.
2.The Latest 6 weeks of data is loaded into Delta_Perf_Weekly_Merch_Level1 from Perf_Weekly_Merch_Level1 table 
  by picking the UPDATED_TS is greater than TIMESTAMP(CURRENT_DATE)  
*/
DELETE `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Perf_Weekly_Merch_Level1` where TRUE;
INSERT
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Delta_Perf_Weekly_Merch_Level1` ( 
	WEEK_ID ,
	MERCH_LEVEL1_ID ,
	STORE_ID ,
	LINEAR_FOOTAGE ,
	SQUARE_FOOTAGE , 
	KPI_1 ,
	KPI_2 ,
	KPI_3 ,
	KPI_4 ,
	KPI_7 ,
	KPI_8 ,
	KPI_9 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS
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
	KPI_7 ,
	KPI_8 ,
	KPI_9 ,
	TOTAL_KPI_1,
	TOTAL_KPI_2,
	TOTAL_KPI_3,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Perf_Weekly_Merch_Level1`
WHERE  
	UPDATED_TS > TIMESTAMP(CURRENT_DATE)
)