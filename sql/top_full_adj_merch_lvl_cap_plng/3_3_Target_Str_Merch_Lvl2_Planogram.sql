/*
Join with Merch_Level2 to populate Merch_Level2_id and generate row number over PLANOGRAM_ID
*/

--Insert Record--
CREATE OR REPLACE TABLE 
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Str_Merch_Lvl2_Planogram` ( 
	PLANOGRAM_ID INT64,
    STORE_ID INT64,
    MERCH_LEVEL2_ID INT64,
    POG_ID STRING,
    POG_ASSIGNMENT_EFF_START_DATE DATE,
    POG_ASSIGNMENT_EFF_END_DATE DATE,
    FLR_PLAN_EFF_START_DATE DATE,
    STATUS INT64,
    FOOTAGE FLOAT64,
    CREATED_USER STRING,
    CREATED_TS TIMESTAMP,
    UPDATED_USER STRING,
    UPDATED_TS TIMESTAMP
) AS

SELECT
	ROW_NUMBER() OVER () PLANOGRAM_ID,
	store.STORE_ID,
	merch.MERCH_LEVEL2_ID,
	Plan.pog_id,
	Plan.Pog_Start_Date ,
	Plan.Pog_End_Date,
	Plan.FLR_Start_Date,
	Plan.Pog_Status,
	Plan.Total_Linear,
	'TOPDTLPLNGFL',
	CURRENT_TIMESTAMP(),
	'TOPDTLPLNGFL',
	CURRENT_TIMESTAMP()
FROM
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Planogram` Plan
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level2` merch ON
	merch.ATTR_3 = CONCAT(Plan.Department,'|||',Plan.SubDepartment)
	AND merch.ATTR_2 = Plan.Sales_Org
	AND merch.ACTIVE_F = 'Y'  
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Store` store ON
	store.STORE_NUMBER = Plan.store_no
	and store.TEXT_1 = Plan.Sales_Org
	and store.ACTIVE_F = 'Y'