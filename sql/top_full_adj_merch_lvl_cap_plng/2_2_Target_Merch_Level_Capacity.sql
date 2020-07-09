/*
join with Merch_Level3 table to populate the MERCH_LEVEL3_ID and build Merch_Level_Capacity
*/

--Insert Record--
CREATE OR REPLACE TABLE   
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level_Capacity` ( 
	  MERCH_LEVEL_ID INT64 
    , POG_CAPACITY FLOAT64 
    , FOOTAGE FLOAT64 
    , CREATED_USER STRING
    , CREATED_TS TIMESTAMP
    , UPDATED_USER STRING
    , UPDATED_TS TIMESTAMP
) AS
SELECT
	merch.MERCH_LEVEL3_ID AS MERCH_LEVEL_ID
	, Capacity
	, Total_Linear
	, 'TOPDTLMERHLVLFL' AS CREATED_USER
	, CURRENT_TIMESTAMP() AS CREATED_TS
	, 'TOPDTLMERHLVLFL' AS UPDATED_USER
	, CURRENT_TIMESTAMP() AS UPDATED_TS
FROM
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Space_Level_Capacity` slc
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level3` merch ON
	merch.ATTR_3 = CONCAT(slc.Department,'|||',slc.SubDepartment,'|||',slc.Category)
	AND merch.ATTR_2 = slc.Sales_Org
	AND merch.ACTIVE_F = 'Y'