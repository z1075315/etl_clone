/*
build the Fiscal_Week from Stage_Fiscal_Week
*/
CREATE OR REPLACE TABLE
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Fiscal_Week` (
Week_id INT64 NOT NULL,
Year INT64 NOT NULL,
Week_Begin_Date DATE NOT NULL,
Week_End_Date DATE NOT NULL,
Week_Of_Year INT64 NOT NULL,
CREATED_USER  String NOT NULL,
CREATED_TS  TIMESTAMP NOT NULL,
UPDATED_USER  String,
UPDATED_TS  TIMESTAMP ) AS
(SELECT
	Week_Id
	,Year
	,Week_Begin_Date
	,Week_End_Date
	,Week_Of_Year
	,"TOPOTFISCWK"
	,CURRENT_TIMESTAMP()
	,"TOPOTFISCWK"
	,CURRENT_TIMESTAMP() 
FROM  
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Fiscal_Week` )