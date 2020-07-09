/*
1. Select distinct department, sub-department and join with Merch_level_1 table to get the MERCH_LEVEL1_ID 
2. generate row number over MERCH_LEVEL2_ID and build MERCH_LEVEL2 table.
*/
DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2` WHERE TRUE;
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2` ( MERCH_LEVEL1_ID,
	MERCH_LEVEL2_ID,
	MERCH_LEVEL2_NAME,
	MERCH_LEVEL2_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL2_ID,
	ATTR_2,
	ATTR_3,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS )
WITH
Hier AS (
SELECT DISTINCT 
	Sales_org,
	Department_Name,
	Department_Desc,
	SubDepartment_Name,
	SubDepartment_Desc
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy` 
),Sub_Dept as 
(
SELECT
	MERCH_LEVEL1_ID,
	SubDepartment_Name as MERCH_LEVEL2_NAME,
	SubDepartment_Desc as MERCH_LEVEL2_DESC,
	'Y' as ACTIVE_F,
	SubDepartment_Name as RETAILER_MERCH_LEVEL2_ID,
	CAST (Sales_org AS STRING) as ATTR_2,
	CONCAT(Department_Name,'|||',SubDepartment_Name) AS ATTR_3,
	'N' as DELETE_F,
	b.banner_id as BANNER_ID,
	'TOPMSTRMERHFL'as CREATED_USER,
	CURRENT_TIMESTAMP() as CREATED_TS,
	'TOPMSTRMERHFL' as UPDATED_USER,
	CURRENT_TIMESTAMP() as UPDATED_TS
FROM
	Hier a
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` b 
ON
	b.RETAILER_BANNER_ID = a.Sales_Org
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` Merch1 
ON
	Merch1.MERCH_LEVEL1_NAME = a.Department_Name
AND Merch1.ATTR_2 = a.Sales_Org

ORDER BY
	ATTR_2,MERCH_LEVEL1_ID,MERCH_LEVEL2_NAME
)
SELECT
	MERCH_LEVEL1_ID,
	ROW_NUMBER() OVER () MERCH_LEVEL2_ID,
	MERCH_LEVEL2_NAME,
	MERCH_LEVEL2_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL2_ID,
	ATTR_2,
	ATTR_3,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS 
FROM
	Sub_Dept
