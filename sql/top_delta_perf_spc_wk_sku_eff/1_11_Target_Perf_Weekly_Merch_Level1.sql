/*
1.The Latest 5 Weeks of Performance Data which is deleted from Perf_Weekly_Merch_Level1 table.
2.The Latest 6 weeks of data(Updated 5 weeks of Data + Current Week Data) is loaded into Perf_Weekly_Merch_Level1 at Department Level. 
*/
DELETE FROM `{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level1` where week_id in 
(SELECT Week_id FROM `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` 
WHERE Week_End_Date <= CURRENT_DATE order by week_id desc limit {{params.Weeks_to_Delete}});

INSERT
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Perf_Weekly_Merch_Level1` ( 
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
	Perf.week_id , 
	Merch.MERCH_LEVEL1_ID ,
	Store.STORE_ID ,
	Perf.Total_Linear,
	Perf.Square,
	IFNULL(Perf.Regular_Sales_Units,0),
	IFNULL(Perf.Regular_sales,0),
	IFNULL(Perf.Promo_Sales_Units,0),
	IFNULL(Perf.Promo_Sales,0),
	IFNULL(Perf.Total_Sales,0),
	IFNULL(Perf.Total_Sales_Units,0), 
	IFNULL(Perf.Total_Gross_Margin,0), 
	IFNULL(Perf.Total_Sales,0) as Total_Sales1,
	IFNULL(Perf.Total_Sales_Units,0) as Total_Sales_Units1, 
	IFNULL(Perf.Total_Gross_Margin,0) as Total_Gross_Margin1,
	'TOPDTLPERFSPCDL',
	CURRENT_TIMESTAMP(),
	'TOPDTLPERFSPCDL',
	CURRENT_TIMESTAMP()  
FROM 
	(SELECT 
		week_id, Sales_org, Store_no,  Department, 
		SUM(Total_Linear) Total_Linear,
		SUM(Square) Square,
		Round(SUM(Regular_Sales_Units),2) Regular_Sales_Units,
		Round(SUM(Regular_sales),2) Regular_sales,
		Round(SUM(Promo_Sales_Units),2) Promo_Sales_Units,
		Round(SUM(Promo_Sales),2) Promo_Sales,
		Round(SUM(Total_Sales_Units),2) Total_Sales_Units, 
		Round(SUM(Total_Sales),2) Total_Sales,
		Round(SUM(Total_Gross_Margin),2) Total_Gross_Margin 
	FROM   
		`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Merch_Hierarchy_Performance_Final` 
	Group by 
		week_id,  Sales_org, Store_no, Department
	) Perf
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level1` Merch ON
	merch.ATTR_2 = Perf.Sales_Org
	AND merch.RETAILER_MERCH_LEVEL1_ID =  Perf.Department
	AND merch.ACTIVE_F = 'Y'
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Store` store ON
	store.STORE_NUMBER = Perf.store_no
	and store.TEXT_1 = Perf.Sales_org
	and store.ACTIVE_F = 'Y'
)