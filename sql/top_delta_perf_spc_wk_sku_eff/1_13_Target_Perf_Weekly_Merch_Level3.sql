/*
1.The Latest 5 Weeks of Performance Data which is deleted from Perf_Weekly_Merch_Level3 table.
2.The Latest 6 weeks of data(Updated 5 weeks of Data + Current Week Data) is loaded into Perf_Weekly_Merch_Level3 
  at Department,Sub_Department,Category Level. 
*/

DELETE FROM `{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level3` where week_id in 
(SELECT Week_id FROM `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` 
WHERE Week_End_Date <= CURRENT_DATE order by week_id desc limit {{params.Weeks_to_Delete}});

INSERT
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level3` ( 
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
	Perf.week_id , 
	Merch.MERCH_LEVEL3_ID ,
	Store.STORE_ID ,
	Perf.Total_Linear,
	Perf.Square,
	IFNULL(Round(Perf.DOS_Error_Pct,2),0) ,
	IFNULL(Perf.Assort_Covrg,0) ,
	IFNULL(Round(Perf.Regular_Sales_Units,2),0),
	IFNULL(Round(Perf.Regular_sales,2),0),
	IFNULL(Round(Perf.Promo_Sales_Units,2),0),
	IFNULL(Round(Perf.Promo_Sales,2),0),
	IFNULL(Round(Perf.Total_Sales,2),0),
	IFNULL(Round(Perf.Total_Sales_Units,2),0), 
	IFNULL(Round(Perf.Total_Gross_Margin,2),0), 
	IFNULL(Round(Perf.Total_Sales,2),0),
	IFNULL(Round(Perf.Total_Sales_Units,2),0), 
	IFNULL(Round(Perf.Total_Gross_Margin,2),0), 
	'TOPDTLPERFSPCDL',
	CURRENT_TIMESTAMP(),
	'TOPDTLPERFSPCDL',
	CURRENT_TIMESTAMP()  
FROM
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Merch_Hierarchy_Performance_Final` Perf
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level3` Merch ON
	merch.ATTR_3 = CONCAT(Perf.Department,'|||',Perf.Sub_Department,'|||',Perf.Category)
	AND merch.ATTR_2 = Perf.Sales_Org
	AND merch.ACTIVE_F = 'Y'
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Store` store ON
	store.STORE_NUMBER = Perf.store_no
	and store.TEXT_1 = Perf.Sales_org
	and store.ACTIVE_F = 'Y'  
)