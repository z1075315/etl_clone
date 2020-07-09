/*
populate Stage_Fiscal_Week  from dim_date_v for fiscalyear greater than or equal to the parameterized year
*/
CREATE OR REPLACE TABLE  
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` (
Week_id INT64,
Week_Year STRING,
Year INT64,
Week_Begin_Date DATE,
Week_End_Date DATE,
Week_Of_Year INT64) AS

(SELECT
	ROW_NUMBER() OVER() AS Week_Id
	,concat(cast(Year as string)
	,format("%02d", Week_Of_Year) ) Week_Year
	,Year
	,Week_Begin_Date
	,Week_End_Date
	,Week_Of_Year
FROM (
SELECT DISTINCT
	FiscalYear Year
	,FiscalWeekStartDate Week_Begin_Date
	,FiscalWeekEndDate Week_End_Date
	,FiscalWeek Week_Of_Year
FROM 
	`{{params.GCP_WOW_ENV}}.adp_dm_masterdata_view.dim_date_v`
WHERE 
	fiscalyear >= {{params.fiscalyear}} ORDER BY FiscalWeekStartDate))